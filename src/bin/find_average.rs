use std::collections::{HashSet, VecDeque};
use std::collections::hash_map::DefaultHasher;
use std::{env, fs};
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::time::Instant;
use hhmmss::Hhmmss;
use num_format::{Locale, ToFormattedString};
use num_format::Locale::{ro, tr};
use rusqlite::{Connection, Statement};

// Rolling Average: Total: 1422 Searched: 229 Average: 6.209606986899563 Not Found: 74

// No Rc: 10.1M Cache - 4.3GB
// Rc: 15M Cache - 8.2GB
// Double Rc:  10.6M - 1.2GB

fn to_titlecase(name: &String) -> String {
    let mut new_name = String::with_capacity(name.len());

    let mut capitalise = true;
    for c in name.chars() {
        if c == ' ' {
            capitalise = true;
            new_name.push(' ');
        }
        else if capitalise {
            new_name.push(c.to_uppercase().next().unwrap());
            capitalise = false;
        }
        else {
            new_name.push(c);
            capitalise = false;
        }
    }

    new_name
}

fn main() {
    let mut searches_total = 0usize;
    let mut searches = 0usize;
    let mut not_found = 0usize;

    loop {
        let db = Connection::open("completed-table.db").unwrap();
        db.execute_batch(
            "PRAGMA synchronous = 0;
              PRAGMA locking_mode = EXCLUSIVE;
              PRAGMA temp_store = MEMORY;
              PRAGMA journal_mode = OFF;
              "
            ,
        ).unwrap();

        let random_start = Instant::now();
        print!("Selecting Random... ");
        
        let starting_at;
        loop {
            let (title, is_redirect): (String, bool) = db.prepare("SELECT title, is_redirect FROM pages ORDER BY RANDOM() LIMIT 1").unwrap()
                .query_row([], |row| {
                    Ok((row.get(0).unwrap(), row.get(1).unwrap()))
                }
                ).unwrap();

            if !is_redirect {
                starting_at = title;
                break;
            }
        }

        let searching_for;
        loop {
            let (title, is_redirect): (String, bool) = db.prepare("SELECT title, is_redirect FROM pages ORDER BY RANDOM() LIMIT 1;").unwrap()
                .query_row([], |row| {
                    Ok((row.get(0).unwrap(), row.get(1).unwrap()))
                }
                ).unwrap();

            if !is_redirect && title != starting_at {
                searching_for = title;
                break;
            }
        }
        
        println!("{:?} - {} -> {}", random_start.elapsed(), starting_at, searching_for);

        let start_id = Page::from_title(starting_at.clone(), false).id;
        let end_id = Page::from_title(searching_for.clone(), false).id;

        let start_time = Instant::now();

        let mut stmt = db.prepare("SELECT destination_id FROM links WHERE source_id = ?").unwrap();

        // let mut cached_query = db.prepare_cached("SELECT destination_id FROM links WHERE source_id = ?").unwrap();
        //
        // for p in [&mut starting_at, &mut searching_for] {
        //     loop {
        //         let res: Result<bool, Error> = cached_query.query_row(
        //             (p.to_owned(),),
        //             |row| Ok(row.get(2).unwrap())
        //         );
        //
        //         if let Ok(is_redirect) = res{
        //             if is_redirect {
        //                 println!("'{p}' is a valid redirect to '{}'", row.0);
        //
        //                 print!("Would you like to use the page this redirect points to? (Y/N): ");
        //                 std::io::stdout().flush().ok();
        //                 let mut r = String::new();
        //                 std::io::stdin().read_line(&mut r).unwrap();
        //
        //                 if r.chars().next().unwrap().to_uppercase().next().unwrap() == 'Y' {
        //                     *p = row.0;
        //                     continue;
        //                 }
        //             }
        //             else {
        //                 println!("'{p}' is a valid page");
        //             }
        //         }
        //         else {
        //             println!("'{p}' is invalid");
        //
        //             print!("Would you like to try title case? (Y/N): ");
        //             std::io::stdout().flush().ok();
        //             let mut r = String::new();
        //             std::io::stdin().read_line(&mut r).unwrap();
        //
        //             if r.chars().next().unwrap().to_uppercase().next().unwrap() == 'Y' {
        //                 *p = to_titlecase(&p);
        //                 continue;
        //             }
        //
        //             print!("Would you like to continue anyway? (Y/N): ");
        //             std::io::stdout().flush().ok();
        //             let mut r = String::new();
        //             std::io::stdin().read_line(&mut r).unwrap();
        //
        //             if r.chars().next().unwrap().to_uppercase().next().unwrap() != 'Y' {
        //                 return;
        //             }
        //         }
        //
        //         break;
        //     }
        // }

        // println!("Finding '{}' -> '{}'", starting_at, searching_for);

        let mut visited: HashSet<LinkedPage> = HashSet::with_capacity(17_000_000);
        visited.insert(LinkedPage::new(start_id, None));

        //? Consider linked list
        let mut open_set = VecDeque::with_capacity(10_000_000);
        open_set.push_back(start_id);

        let mut count: u32 = 0;

        'main_loop: loop {
            let page = open_set.pop_front().unwrap();
            // if open_set.is_empty() && visited.len() != 1 {
            //     // println!("Last page: {}", page.to_str());
            // }

            count += 1;
            if count % 10_000 == 0 {
                println!(
                    "Pages searched: {} [{:?}/page] | Cache size: {} | Open set size: {}",
                    count.to_formatted_string(&Locale::en),
                    start_time.elapsed() / count,
                    visited.len().to_formatted_string(&Locale::en),
                    open_set.len().to_formatted_string(&Locale::en),
                );
            }

            let links = get_links(page, &mut stmt);

            for link in links {
                if !visited.insert(LinkedPage::new(link, Some(page))) {
                    continue;
                }

                if link == end_id {
                    println!("Final path: (Capitalisation of words may be incorrect)");
                    println!("{}", LinkedPage::new(link, Some(page)).unwind(&visited, &db));
                    searches_total += LinkedPage::new(link, Some(page)).count(&visited);
                    searches += 1;
                    break 'main_loop;
                }

                open_set.push_back(link);
            }

            if open_set.is_empty() {
                let mut file = fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .append(true)
                    .open("not_found.txt")
                    .unwrap();
                
                write!(file, "{} -> {}\n", starting_at, searching_for);
                
                println!("No more pages!");
                not_found += 1;
                break;
            }
        }

        println!("Completed in {}", start_time.elapsed().hhmmssxxx());
        println!(
            "Pages searched: {} [{:?}/page] | Cache size: {} | Open set size: {}",
            count.to_formatted_string(&Locale::en),
            start_time.elapsed() / count,
            visited.len().to_formatted_string(&Locale::en),
            open_set.len().to_formatted_string(&Locale::en),
        );

        println!("Rolling Average: Total: {} Searched: {} Average: {} Not Found: {}", searches_total, searches, ((searches_total as f64) / (searches as f64)), not_found)
    }
}

fn get_links(source_id: i64, stmt: &mut Statement) -> Vec<i64> {
    stmt.query_map([source_id], |row|
        Ok(row.get(0)?)
    ).unwrap().map(|x| x.unwrap()).collect()
}

pub struct Page {
    pub id: i64,
    pub from_redirect: bool,
}

impl Page {
    pub fn new(id: i64, from_redirect: bool) -> Page {
        Page {
            id,
            from_redirect,
        }
    }

    pub fn from_title(mut title: String, from_redirect: bool) -> Page {
        let mut hasher = DefaultHasher::new();
        title.make_ascii_lowercase();
        title.hash(&mut hasher);
        Page {
            id: i64::from_ne_bytes(hasher.finish().to_ne_bytes()),
            from_redirect
        }
    }
}

impl Hash for Page {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_i64(self.id);
    }
}

impl PartialEq for Page {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Page {}

pub struct LinkedPage {
    pub page: i64,
    pub from: Option<i64>
}

impl LinkedPage {
    pub fn new(page: i64, from: Option<i64>) -> LinkedPage {
        LinkedPage {
            page,
            from
        }
    }

    pub fn count(&self, others: &HashSet<LinkedPage>) -> usize {
        if self.from.is_none() {
            return 1;
        }
        return 1 + others.get(&LinkedPage::new(self.from.unwrap(), None)).unwrap().count(others);
    }

    pub fn fmt_title(title: String, redirect: bool) -> String {
        let mut output = to_titlecase(&title);
        if redirect {
            output += " =?=>";
        } else {
            output += " --->"
        }
        output
    }

    pub fn unwind(&self, others: &HashSet<LinkedPage>, conn: &Connection) -> String {
        let mut stmt = conn.prepare("SELECT title, is_redirect FROM pages WHERE id = ?").unwrap();

        let mut output = String::new();

        let (title, _redirect): (String, bool) = stmt.query_row([self.page], |row| Ok((row.get(0).unwrap(), row.get(1).unwrap()))).unwrap();
        output = to_titlecase(&title) + output.as_str();

        let mut from = self.from;

        while from.is_some() {
            let (title, redirect): (String, bool) = stmt.query_row([from.unwrap()], |row| Ok((row.get(0).unwrap(), row.get(1).unwrap()))).unwrap();
            output = Self::fmt_title(title, redirect) + "\n" + output.as_str();

            from = others.get(&LinkedPage::new(from.unwrap(), None)).unwrap().from;
        }

        output
    }
}

impl Hash for LinkedPage {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_i64(self.page);
    }
}

impl PartialEq for LinkedPage {
    fn eq(&self, other: &Self) -> bool {
        self.page == other.page
    }
}

impl Eq for LinkedPage {}