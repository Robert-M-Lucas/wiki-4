use std::cmp::min;
use std::collections::hash_map::DefaultHasher;
use std::fs;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader};
use std::time::Instant;
use hhmmss::Hhmmss;
use rusqlite::{Connection, Error, ErrorCode, ToSql};

fn main() {
    let start = Instant::now();
    let file = File::open("enwiki-20231101-pages-articles-multistream.xml").unwrap();
    let reader = BufReader::new(file);

    let mut db = DB::new(1000, 1_000_000, 75_000_000);

    let mut count: u32 = 0;
    const TOTAL_ARTICLES: u32 = 23_100_000;

    let mut lines = reader.lines();

    const TITLE_TAG: &str = "    <title>";
    const END_TITLE_TAG: &str = "</title>";
    const TEXT_TAG: &str = "      <text";
    const END_TEXT_TAG: &str = "</text>";

    'main_loop: loop {
        let title;
        loop {
            let line = match lines.next() {
                Some(Ok(line)) => line,
                Some(Err(e)) => {
                    println!("Breaking main loop due to error reading line: {:?}", e);
                    break 'main_loop;
                }
                None => {{
                    println!("No more lines");
                    break 'main_loop;
                }}
            };
            if line.len() < TITLE_TAG.len() || !line.is_char_boundary(TITLE_TAG.len()) || &line[..TITLE_TAG.len()] != TITLE_TAG {
                continue;
            }

            title = line[TITLE_TAG.len()..line.len() - END_TITLE_TAG.len()].to_string();
            break;
        }

        for pattern in FORBIDDEN_PATTERNS {
            if title.find(pattern).is_some() {
                continue 'main_loop;
            }
        }


        let mut body = String::with_capacity(30);
        loop {
            let line = match lines.next() {
                Some(Ok(line)) => line,
                Some(Err(e)) => {
                    println!("Breaking main loop due to error reading line: {:?}", e);
                    break 'main_loop;
                }
                None => {{
                    println!("No more lines");
                    break 'main_loop;
                }}
            };
            if line.len() < TEXT_TAG.len() || !line.is_char_boundary(TEXT_TAG.len()) || &line[..TEXT_TAG.len()] != TEXT_TAG {
                continue;
            }

            let start = line.find('>');
            let mut line_owned = line[(start.unwrap() + '>'.len_utf8())..].to_string();
            let mut line = line_owned.as_str();
            loop {
                let mut end = false;
                if line.len() >= END_TEXT_TAG.len() &&
                    line.is_char_boundary(line.len() - END_TEXT_TAG.len()) &&
                    &line[(line.len() - END_TEXT_TAG.len())..] == END_TEXT_TAG {
                    end = true;
                    line = &line[..line.len() - END_TEXT_TAG.len()];
                }

                body += line;
                if end { break; }
                else { body.push('\n'); }
                line_owned = lines.next().unwrap().unwrap();
                line = line_owned.as_str();
            }
            break;
        }

        let (links, is_redirect) = match get_links_from_body(body, &title) {
            Ok(links) => links,
            Err(e) => {
                println!("{}", e);
                continue;
            }
        };

        // db.cache(title.to_lowercase(), links, is_redirect);
        db.cache(title, links, is_redirect);

        count += 1;
        if count % 100_000 == 0 {
            if count < TOTAL_ARTICLES {
                println!("Completed {} articles in {} [{:?}/article]. ETA: {}", count, start.elapsed().hhmmss(), start.elapsed() / count, ((start.elapsed() / count) * (TOTAL_ARTICLES - count)).hhmmss())
            }
            else {
                println!("Completed {} articles in {} [{:?}/article]", count, start.elapsed().hhmmss(), start.elapsed() / count);
            }
        }
    }

    db.write_pages_to_db();
    db.write_links_to_db();
    // db.resolve_links();

    drop(db);
    fs::rename("table.db", "completed-table.db").unwrap();

    println!("Completed {} articles in {} [{:?}/article]", count, start.elapsed().hhmmss(), start.elapsed() / count);
}

struct DB {
    conn: Connection,
    batch_size: usize,
    pages_insert_threshold: usize,
    links_insert_threshold: usize,
    pages_to_insert: Vec<(i64, String, bool)>,
    links_to_insert: Vec<(i64, i64)>
}

impl DB {
    pub fn new(batch_size: usize, pages_insert_threshold: usize, links_insert_threshold: usize) -> Self {
        let conn = Connection::open("table.db").unwrap();

        conn.execute_batch(
            "PRAGMA journal_mode = OFF;
              PRAGMA synchronous = OFF;
              PRAGMA cache_size = 1000000;
              PRAGMA locking_mode = EXCLUSIVE;
              PRAGMA temp_store = MEMORY;",
        ).unwrap();

        conn.execute("DROP TABLE IF EXISTS pages", ()).unwrap();

        conn.execute("DROP TABLE IF EXISTS links", ()).unwrap();

        conn.execute(
            "CREATE TABLE IF NOT EXISTS pages (
                id INTEGER PRIMARY KEY,
                title TEXT,
                is_redirect BOOLEAN
            )",
           ()
        ).unwrap();

        // conn.execute(
        //     "CREATE TABLE IF NOT EXISTS links_temp (
        //         source_id INTEGER,
        //         destination TEXT,
        //         PRIMARY KEY(source_id, destination)
        //     )",
        //     ()
        // ).unwrap();

        conn.execute(
            "CREATE TABLE IF NOT EXISTS links (
                source_id INTEGER,
                destination_id INTEGER,
                PRIMARY KEY(source_id, destination_id)
            )",
           ()
        ).unwrap();

        Self {
            conn,
            batch_size,
            pages_insert_threshold,
            links_insert_threshold,
            pages_to_insert: Vec::with_capacity(pages_insert_threshold + (pages_insert_threshold / 100)),
            links_to_insert: Vec::with_capacity(links_insert_threshold + (links_insert_threshold / 100))
        }
    }

    pub fn write_pages_to_db(&mut self) {
        let tx = self.conn.transaction().unwrap();

        if self.pages_to_insert.len() == 0 {
            println!("Cancelling db write as page cache is empty");
            return;
        }

        let start = Instant::now();
        println!("Writing {} pages to database", self.pages_to_insert.len());

        let mut cached_statement =
            tx.prepare_cached(
                format!("INSERT INTO pages VALUES {}", " (?, ?, ?),".repeat(self.batch_size - 1) + " (?, ?, ?)")
                    .as_str()).unwrap();

        let mut individual_cached_statement =
            tx.prepare_cached("INSERT INTO pages VALUES (?, ?, ?)").unwrap();

        let mut params = Vec::with_capacity(self.batch_size * 3);
        let (batchable, non_batchable) = self.pages_to_insert.split_at(
            self.pages_to_insert.len() - (self.pages_to_insert.len() % self.batch_size)
        );

        let mut count = 0;
        for data in batchable {
            params.push(&data.0 as &dyn ToSql);
            params.push(&data.1 as &dyn ToSql);
            params.push(&data.2 as &dyn ToSql);
            count += 1;
            if count == self.batch_size {
                if let Err(e) = cached_statement.execute(&*params) {
                    println!("Database pages batch failed due to error - retrying one at a time: {:?}", e);

                    for params in params.chunks(3) {
                        if let Err(e) = individual_cached_statement.execute(params) {
                            // let id = match params[0].to_sql().unwrap()
                            // {
                            //     ToSqlOutput::Borrowed(ValueRef::Integer(value)) => value,
                            //     _ => panic!()
                            // };
                            //
                            // let title = match params[1].to_sql().unwrap()
                            // {
                            //     ToSqlOutput::Borrowed(ValueRef::Text(value)) => String::from_utf8_lossy(value),
                            //     _ => panic!()
                            // };

                            println!(
                                "Database page insert on data [{:?}, {:?}, {:?}] failed due to error: {:?}",
                                params[0].to_sql().unwrap(),
                                params[1].to_sql().unwrap(),
                                params[2].to_sql().unwrap(),
                                e
                            );

                            // let result = self.conn.execute("INSERT INTO page_reference_errors VALUES (?, ?, ?)", params);
                            // if result.is_err() { println!("{:?}", result.unwrap_err()); }
                        }
                    }
                }
                params = Vec::with_capacity(self.batch_size * 3);
                count = 0;
            }
        }

        if non_batchable.len() > 0 {
            for data in non_batchable {
                if let Err(e) = individual_cached_statement.execute((&data.0, &data.1, &data.2)) {
                    println!(
                        "Database pages insert on data [{:?}, {:?}, {:?}] failed due to error: {:?}",
                        data.0,
                        data.1,
                        data.2,
                        e
                    );

                    // let result = self.conn.execute("INSERT INTO page_reference_errors VALUES (?, ?, ?)", (&data.0, &data.1, &data.2));
                    // if result.is_err() { println!("{:?}", result.unwrap_err()); }
                }
            }
        }

        drop(cached_statement);
        drop(individual_cached_statement);

        tx.commit().unwrap();

        self.pages_to_insert = Vec::with_capacity(self.pages_insert_threshold + (self.pages_insert_threshold / 100));
        println!("Finished writing pages to database in {:?}", start.elapsed());
    }

    fn is_unique_error(e: &Error) -> bool {
        e.sqlite_error_code().is_some_and(
            |c| c == ErrorCode::ConstraintViolation
        )
    }

    pub fn write_links_to_db(&mut self) {
        let tx = self.conn.transaction().unwrap();

        if self.links_to_insert.len() == 0 {
            println!("Cancelling db write as link cache is empty");
            return;
        }

        let start = Instant::now();
        println!("Writing {} links to database", self.links_to_insert.len());

        let mut cached_statement =
            tx.prepare_cached(
                format!("INSERT INTO links VALUES {}", " (?, ?),".repeat(self.batch_size - 1) + " (?, ?)")
                    .as_str()).unwrap();

        let mut individual_cached_statement =
            tx.prepare_cached("INSERT INTO links VALUES (?, ?)").unwrap();

        let mut params = Vec::with_capacity(self.batch_size * 2);
        let (batchable, non_batchable) = self.links_to_insert.split_at(
            self.links_to_insert.len() - (self.links_to_insert.len() % self.batch_size)
        );

        let mut count = 0;
        for data in batchable {
            params.push(&data.0 as &dyn ToSql);
            params.push(&data.1 as &dyn ToSql);
            count += 1;
            if count == self.batch_size {
                if let Err(e) = cached_statement.execute(&*params) {
                    if !Self::is_unique_error(&e) {
                        println!("Database links batch failed due to error - retrying one at a time: {:?}", e);
                    }

                    for params in params.chunks(2) {
                        if let Err(e) = individual_cached_statement.execute(params) {
                            // let source_id = match params[0].to_sql().unwrap()
                            // {
                            //     ToSqlOutput::Owned(Value::Integer(value)) => value,
                            //     _ => panic!()
                            // };
                            //
                            // let destination = match params[1].to_sql().unwrap()
                            // {
                            //     ToSqlOutput::Borrowed(ValueRef::Text(value)) => String::from_utf8_lossy(value),
                            //     _ => panic!()
                            // };

                            if !Self::is_unique_error(&e) {
                                println!(
                                    "Database link insert on data [{:?}, {:?}] failed due to error: {:?}",
                                    params[0].to_sql(),
                                    params[1].to_sql(),
                                    e
                                );
                            }

                            // let result = self.conn.execute("INSERT INTO page_reference_errors VALUES (?, ?, ?)", params);
                            // if result.is_err() { println!("{:?}", result.unwrap_err()); }
                        }
                    }
                }
                params = Vec::with_capacity(self.batch_size * 2);
                count = 0;
            }
        }

        if non_batchable.len() > 0 {
            for data in non_batchable {
                if let Err(e) = individual_cached_statement.execute((&data.0, &data.1)) {
                    if !Self::is_unique_error(&e) {
                        println!(
                            "Database pages insert on data [{:?}, {:?}] failed due to error: {:?}",
                            data.0,
                            data.1,
                            e
                        );
                    }

                    // let result = self.conn.execute("INSERT INTO page_reference_errors VALUES (?, ?, ?)", (&data.0, &data.1, &data.2));
                    // if result.is_err() { println!("{:?}", result.unwrap_err()); }
                }
            }
        }

        drop(cached_statement);
        drop(individual_cached_statement);

        tx.commit().unwrap();

        self.links_to_insert = Vec::with_capacity(self.links_insert_threshold + (self.links_insert_threshold / 100));
        println!("Finished writing links to database in {:?}", start.elapsed());
    }

    pub fn cache(&mut self, title: String, links: Vec<String>, is_redirect: bool) {
        let mut hasher = DefaultHasher::new();
        let lower_title = title.to_ascii_lowercase();
        lower_title.hash(&mut hasher);
        let title_hash = i64::from_ne_bytes(hasher.finish().to_ne_bytes());

        self.pages_to_insert.push((title_hash, title, is_redirect));

        for mut link in links {
            link.make_ascii_lowercase();
            let mut hasher = DefaultHasher::new();
            link.hash(&mut hasher);
            let link_hash = i64::from_ne_bytes(hasher.finish().to_ne_bytes());
            self.links_to_insert.push((title_hash, link_hash));
        }

        if self.pages_to_insert.len() >= self.pages_insert_threshold {
            self.write_pages_to_db();
        }
        if self.links_to_insert.len() >= self.links_insert_threshold {
            self.write_links_to_db();
        }
    }

    // pub fn resolve_links(&mut self) {
    //     let start = Instant::now();
    //     println!("Resolving links");
    //
    //     struct Row {
    //         source_id: i64,
    //         destination: String
    //     }
    //
    //     let mut stmt = self.conn.prepare("SELECT * FROM links_temp").unwrap();
    //     let link_iter = stmt.query_map([], |row|
    //         Ok(Row {
    //             source_id: row.get(0)?,
    //             destination: row.get(1)?,
    //         })
    //     ).unwrap().map(|x| x.unwrap());
    //
    //     let mut query =
    //         self.conn.prepare_cached("SELECT id FROM pages WHERE title = ?").unwrap();
    //
    //     let mut insert =
    //         self.conn.prepare_cached("INSERT INTO links VALUES (?, ?)").unwrap();
    //
    //     for row in link_iter {
    //         let destination_id: i64 = query.query_row([&row.destination], |row| Ok(row.get(0))).unwrap().unwrap();
    //         insert.execute([row.source_id, destination_id]).unwrap();
    //     }
    //
    //     self.conn.execute("DROP TABLE links_temp", ()).unwrap();
    //
    //     println!("Finished resolving links in {:?}", start.elapsed().hhmmss());
    // }
}

const REDIRECT_TEXT: &str = "#REDIRECT [[";
const FORBIDDEN_PATTERNS: [&str; 10] = [
    "Wikipedia:",
    "Category:",
    "File:",
    "Special:",
    "Template:",
    "Template_talk:",
    "User:",
    "WP:",
    "Help:",
    "File:",
    // "Portal:",
];

const SEE_ALSO: &str = "==See also==";
const REFERENCES: &str = "==References==";
fn get_links_from_body(body: String, title: &String) -> Result<(Vec<String>, bool), String> {
    return if body.len() > REDIRECT_TEXT.len() && body.is_char_boundary(REDIRECT_TEXT.len()) && &body[..REDIRECT_TEXT.len()] == REDIRECT_TEXT {
        let end = body.find("]]");
        if let Some(end) = end {
            let redirect = body[REDIRECT_TEXT.len()..end].trim();
            let redirect = redirect.split('#').nth(0).unwrap().trim();
            for pattern in FORBIDDEN_PATTERNS {
                if redirect.len() >= pattern.len() && redirect.is_char_boundary(pattern.len()) && &redirect[..pattern.len()] == pattern {
                    return Ok((Vec::new(), true));
                }
            }
            Ok((vec![redirect.to_string()], true))
        } else {
            Err(format!("Getting redirect link from '{}' failed", title))
        }
    } else {
        let mut references = Vec::new();

        let limit = body.find(SEE_ALSO).or_else(|| body.find(REFERENCES)).unwrap_or(body.len());
        let body = &body[..limit];
        'link_search_loop: for (link_pos, _) in body.match_indices("[[") {
            let after_link_start = &body[link_pos + "[[".len()..];
            let end1 = after_link_start.find('|');
            let end2 = after_link_start.find(']');
            let end = if end1.is_some() && end2.is_some() {
                Some(min(end1.unwrap(), end2.unwrap()))
            }
            else {
                end1.or_else(|| end2)
            };

            if let Some(end) = end {
                let mut link = after_link_start[..end].trim();
                for pattern in FORBIDDEN_PATTERNS {
                    if link.len() >= pattern.len() && link.is_char_boundary(pattern.len()) && &link[..pattern.len()] == pattern {
                        continue 'link_search_loop;
                    }
                }

                if let Some(pos) = link.find('#') {
                    if pos == 0 {
                        continue;
                    }
                    link = &link[..pos];
                }

                references.push(link.to_string());
            } else {
                break;
            }
        }

        Ok((references, false))
    }
}