use std::time::Instant;
use hhmmss::Hhmmss;
use rusqlite::Connection;

const TOTAL_ARTICLES: u32 = 23_100_000;

fn main() {
    println!("Opening connection");
    let conn = Connection::open("reference-count-table.db").unwrap();
    println!("Configuring connection");
    conn.execute_batch(
        "PRAGMA journal_mode = OFF;
              PRAGMA cache_size = 1000000;
              PRAGMA locking_mode = EXCLUSIVE;
              PRAGMA temp_store = MEMORY;",
    ).unwrap();
    println!("Dropping column");
    if let Err(e) = conn.execute("ALTER TABLE page_references DROP COLUMN reference_count", ()) {
        println!("Drop column failed with error {:?}", e);
    }
    println!("Adding column");
    conn.execute("ALTER TABLE page_references ADD COLUMN reference_count INTEGER DEFAULT 0", ()).unwrap();

    let mut count: u32 = 0;
    let start = Instant::now();

    let mut cached_update_statement = conn.prepare_cached("UPDATE page_references SET reference_count = reference_count + 1 WHERE title = ?").unwrap();

    let mut statement = conn.prepare_cached("SELECT links FROM page_references").unwrap();
    let mut rows = statement.query(()).unwrap();
    let mut row = rows.next().unwrap();

    while row.is_some() {
        let links: String = row.unwrap().get(0).unwrap();

        for link in links.split("<|>") {
            if link.is_empty() {
                continue;
            }

            let mut result =
                cached_update_statement.execute(
                (link,)
            );

            if let Err(_) = result {
                let mut v: Vec<char> = link.chars().collect();
                v[0] = v[0].to_uppercase().nth(0).unwrap();
                let link: String = v.into_iter().collect();

                result = cached_update_statement.execute(
                    (link,)
                );
            }

            if let Err(e) = result {
                println!("Updating reference count failed: {:?}", e);
            }
        }

        count += 1;
        if count % 1000 == 0 {
            if count < TOTAL_ARTICLES {
                println!("{} pages completed in {} [{:?}/page]. ETA: {}", count, start.elapsed().hhmmss(), start.elapsed() / count, ((start.elapsed() / count) * (TOTAL_ARTICLES - count)).hhmmss())

            }
            else {
                println!("{} pages completed in {} [{:?}/page]", count, start.elapsed().hhmmss(), start.elapsed() / count);
            }
        }

        row = rows.next().unwrap();
    }


}