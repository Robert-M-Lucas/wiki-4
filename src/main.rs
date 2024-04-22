use std::collections::{HashSet, VecDeque};
use std::collections::hash_map::DefaultHasher;
use std::env;
use std::hash::{Hash, Hasher};
use std::time::Instant;
use hhmmss::Hhmmss;
use num_format::{Locale, ToFormattedString};
use rusqlite::{Connection, Statement};


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

fn get_links(source_id: i64, stmt: &mut Statement) -> Vec<i64> {
    stmt.query_map([source_id], |row|
        Ok(row.get(0)?)
    ).unwrap().map(|x| x.unwrap()).collect()
}

fn main() {
    let mut args: Vec<String> = env::args().into_iter().collect();

    // ! CASE SENSITIVE
    // let starting_at = "Tobi 12";
    // let searching_for = "xxINVALIDxx";

    let (starting_at, searching_for) = if args.len() >= 3 {
        let b = args.remove(2);
        let a = args.remove(1);
        (a, b)
    }
    else {
        ("Bedford".to_string(), "Cneoridium dumosum (Nuttall) Hooker F. Collected March 26, 1960, at an Elevation of about 1450 Meters on Cerro Quemazón, 15 Miles South of Bahía de Los Angeles, Baja California, México, Apparently for a Southeastward Range Extension of Some 140 Miles".to_string())
    };

    drop(args);

    let start_id = Page::from_title(starting_at, false).id;
    let end_id = Page::from_title(searching_for, false).id;

    let start_time = Instant::now();

    let db = Connection::open("completed-table.db").unwrap();
    db.execute_batch(
        "PRAGMA synchronous = 0;
              PRAGMA locking_mode = EXCLUSIVE;
              PRAGMA temp_store = MEMORY;
              PRAGMA journal_mode = OFF;"
        ,
    ).unwrap();

    let mut stmt = db.prepare("SELECT destination_id FROM links WHERE source_id = ?").unwrap();


    let mut visited: HashSet<LinkedPage> = HashSet::with_capacity(17_000_000);
    visited.insert(LinkedPage::new(start_id, None));

    //? Consider linked list
    let mut open_set = VecDeque::with_capacity(7_500_000);
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
                "Pages searched: {} [{:?}/page] | Seen: {} | Open set size: {}",
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
                break 'main_loop;
            }

            open_set.push_back(link);
        }

        if open_set.is_empty() {
            println!("No more pages!");
            break;
        }
    }

    println!("Completed in {}", start_time.elapsed().hhmmssxxx());
    println!(
        "Pages searched: {} [{:?}/page] | Seen: {} | Open set size: {}",
        count.to_formatted_string(&Locale::en),
        start_time.elapsed() / count,
        visited.len().to_formatted_string(&Locale::en),
        open_set.len().to_formatted_string(&Locale::en),
    );
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

    pub fn fmt_title(title: String, redirect: bool) -> String {
        let mut output = to_titlecase(&title);
        if redirect {
            output += " =?=>";
        }
        else {
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