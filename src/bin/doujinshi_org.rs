use std::sync::{Arc, Mutex};

use futures::future;
use glob::glob;
use indicatif::ParallelProgressIterator;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

async fn process_books() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let writer = Arc::new(Mutex::new(csv::Writer::from_path(
        "data/doujinshi.org/books.csv",
    )?));
    writer
        .lock()
        .unwrap()
        .write_record(&["id", "name", "name_en", "name_romaji", "name_alt"])?;

    glob("data/doujinshi.org/Book/*.json")?
        .filter_map(|entry| entry.ok())
        .collect::<Vec<_>>()
        .par_iter()
        .progress()
        .for_each(|path| {
            if let Ok(file) = std::fs::File::open(path) {
                let reader = std::io::BufReader::new(file);
                if let Ok(data) = serde_json::from_reader::<_, serde_json::Value>(reader) {
                    let mut writer = writer.lock().unwrap();
                    let _ = writer.write_record(&[
                        data["@ID"].as_str().unwrap_or_default().replace("B", ""),
                        data["NAME_JP"].as_str().unwrap_or_default().to_string(),
                        data["NAME_EN"].as_str().unwrap_or_default().to_string(),
                        data["NAME_R"].as_str().unwrap_or_default().to_string(),
                        data["NAME_ALT"]
                            .as_array()
                            .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>())
                            .unwrap_or_else(|| vec![data["NAME_ALT"].as_str().unwrap_or_default()])
                            .join("|"),
                    ]);
                }
            }
        });

    writer.lock().unwrap().flush()?;
    Ok(())
}

async fn process_contents() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let writer = Arc::new(Mutex::new(csv::Writer::from_path(
        "data/doujinshi.org/contents.csv",
    )?));
    writer
        .lock()
        .unwrap()
        .write_record(&["id", "name", "name_en", "name_romaji", "name_alt"])?;

    glob("data/doujinshi.org/Content/*.json")?
        .filter_map(|entry| entry.ok())
        .collect::<Vec<_>>()
        .par_iter()
        .progress()
        .for_each(|path| {
            if let Ok(file) = std::fs::File::open(path) {
                let reader = std::io::BufReader::new(file);
                if let Ok(data) = serde_json::from_reader::<_, serde_json::Value>(reader) {
                    let mut writer = writer.lock().unwrap();
                    let _ = writer.write_record(&[
                        data["@ID"].as_str().unwrap_or_default().replace("K", ""),
                        data["NAME_JP"].as_str().unwrap_or_default().to_string(),
                        data["NAME_EN"].as_str().unwrap_or_default().to_string(),
                        data["NAME_R"].as_str().unwrap_or_default().to_string(),
                        data["NAME_ALT"]
                            .as_array()
                            .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>())
                            .unwrap_or_else(|| vec![data["NAME_ALT"].as_str().unwrap_or_default()])
                            .join("|"),
                    ]);
                }
            }
        });

    writer.lock().unwrap().flush()?;
    Ok(())
}

async fn process_authors() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let writer = Arc::new(Mutex::new(csv::Writer::from_path(
        "data/doujinshi.org/authors.csv",
    )?));
    writer
        .lock()
        .unwrap()
        .write_record(&["id", "name", "name_en", "name_romaji", "name_alt"])?;

    glob("data/doujinshi.org/Author/*.json")?
        .filter_map(|entry| entry.ok())
        .collect::<Vec<_>>()
        .par_iter()
        .progress()
        .for_each(|path| {
            if let Ok(file) = std::fs::File::open(path) {
                let reader = std::io::BufReader::new(file);
                if let Ok(data) = serde_json::from_reader::<_, serde_json::Value>(reader) {
                    let mut writer = writer.lock().unwrap();
                    let _ = writer.write_record(&[
                        data["@ID"].as_str().unwrap_or_default().replace("A", ""),
                        data["NAME_JP"].as_str().unwrap_or_default().to_string(),
                        data["NAME_EN"].as_str().unwrap_or_default().to_string(),
                        data["NAME_R"].as_str().unwrap_or_default().to_string(),
                        // name_alt could be a string or an array
                        data["NAME_ALT"]
                            .as_array()
                            .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>())
                            .unwrap_or_else(|| vec![data["NAME_ALT"].as_str().unwrap_or_default()])
                            .join("|"),
                    ]);
                }
            }
        });

    writer.lock().unwrap().flush()?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tasks = vec![
        tokio::spawn(process_authors()),
        tokio::spawn(process_contents()),
        tokio::spawn(process_books()),
    ];

    future::join_all(tasks).await;

    Ok(())
}
