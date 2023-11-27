use std::fs::File;
use std::io::{BufRead, BufReader};

fn main() {
    let file = File::open("enwiki-20231101-pages-articles-multistream.xml").unwrap();
    let mut reader = BufReader::new(file);

    let mut i = i32::MAX;
    while i > 0 {
        let mut buffer = Vec::new();
        reader.read_until('\n' as u8, &mut buffer).unwrap();
        let string = String::from_utf8(buffer).unwrap();
        if string.contains("Albrecht Achilles") {
            i = 20;
        }
        print!("{}", string);
        i -= 1;
    }
    return;
}