use std::sync::{Arc, Mutex};

use futures::future;
use glob::glob;
use indicatif::ParallelProgressIterator;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

fn array_or_string(value: &serde_json::Value) -> String {
    value
        .as_array()
        .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>())
        .unwrap_or_else(|| vec![value.as_str().unwrap_or_default()])
        .join(",")
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
                    let _ = writer.lock().unwrap().write_record(&[
                        data["@ID"].as_str().unwrap_or_default().replace("A", ""),
                        data["NAME_JP"].as_str().unwrap_or_default().to_string(),
                        data["NAME_EN"].as_str().unwrap_or_default().to_string(),
                        data["NAME_R"].as_str().unwrap_or_default().to_string(),
                        array_or_string(&data["NAME_ALT"]),
                    ]);
                }
            }
        });

    writer.lock().unwrap().flush()?;
    Ok(())
}

async fn process_books() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let writer = Arc::new(Mutex::new(csv::Writer::from_path(
        "data/doujinshi.org/books.csv",
    )?));
    writer.lock().unwrap().write_record(&[
        "id",
        "type",
        "name",
        "name_en",
        "name_romaji",
        "release_date",
        "isbn",
        "pages",
        "language",
        "description",
        "is_adult",
        "is_copybook",
        "is_anthology",
        "is_magazine",
    ])?;

    glob("data/doujinshi.org/Book/*.json")?
        .filter_map(|entry| entry.ok())
        .collect::<Vec<_>>()
        .par_iter()
        .progress()
        .for_each(|path| {
            if let Ok(file) = std::fs::File::open(path) {
                let reader = std::io::BufReader::new(file);
                if let Ok(data) = serde_json::from_reader::<_, serde_json::Value>(reader) {
                    let _ = writer.lock().unwrap().write_record(&[
                        data["@ID"].as_str().unwrap_or_default().replace("B", ""),
                        data["@TYPE"].as_str().unwrap_or_default().to_string(),
                        data["NAME_JP"].as_str().unwrap_or_default().to_string(),
                        data["NAME_EN"].as_str().unwrap_or_default().to_string(),
                        data["NAME_R"].as_str().unwrap_or_default().to_string(),
                        data["DATE_RELEASED"]
                            .as_str()
                            .unwrap_or_default()
                            .to_string(),
                        data["DATA_ISBN"].as_str().unwrap_or_default().to_string(),
                        data["DATA_PAGES"].as_str().unwrap_or_default().to_string(),
                        data["DATA_LANGUAGE"]
                            .as_str()
                            .unwrap_or_default()
                            .to_string(),
                        data["DATA_INFO"].as_str().unwrap_or_default().to_string(),
                        data["DATA_AGE"].as_str().unwrap_or_default().to_string(),
                        data["DATA_COPYSHI"]
                            .as_str()
                            .unwrap_or_default()
                            .to_string(),
                        data["DATA_ANTHOLOGY"]
                            .as_str()
                            .unwrap_or_default()
                            .to_string(),
                        data["DATA_MAGAZINE"]
                            .as_str()
                            .unwrap_or_default()
                            .to_string(),
                    ]);
                }
            }
        });

    writer.lock().unwrap().flush()?;
    Ok(())
}

async fn process_characters() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let writer = Arc::new(Mutex::new(csv::Writer::from_path(
        "data/doujinshi.org/characters.csv",
    )?));
    let tags_writer = Arc::new(Mutex::new(csv::Writer::from_path(
        "data/doujinshi.org/character_tags.csv",
    )?));
    writer.lock().unwrap().write_record(&[
        "id",
        "name",
        "name_en",
        "name_romaji",
        "name_alt",
        "sex",
        "age",
    ])?;
    tags_writer
        .lock()
        .unwrap()
        .write_record(&["character_id", "tag_id"])?;

    glob("data/doujinshi.org/Character/*.json")?
        .filter_map(|entry| entry.ok())
        .collect::<Vec<_>>()
        .par_iter()
        .progress()
        .for_each(|path| {
            if let Ok(file) = std::fs::File::open(path) {
                let reader = std::io::BufReader::new(file);
                if let Ok(data) = serde_json::from_reader::<_, serde_json::Value>(reader) {
                    let id = data["@ID"].as_str().unwrap_or_default().replace("H", "");
                    let _ = writer.lock().unwrap().write_record(&[
                        id.clone(),
                        data["NAME_JP"].as_str().unwrap_or_default().to_string(),
                        data["NAME_EN"].as_str().unwrap_or_default().to_string(),
                        data["NAME_R"].as_str().unwrap_or_default().to_string(),
                        array_or_string(&data["NAME_ALT"]),
                        data["DATA_SEX"].as_str().unwrap_or_default().to_string(),
                        data["DATA_AGE"].as_str().unwrap_or_default().to_string(),
                    ]);

                    // Write character tags
                    // LINKS.ITEM could be a list of tags or a single tag
                    if let Some(links) = data["LINKS"].as_object() {
                        // Check if the item is an array or a single object
                        // If it's an array, iterate over it
                        // If it's a single object, wrap it in a vector
                        links["ITEM"]
                            .as_array()
                            .map(|arr| arr.iter().filter_map(|v| v.as_object()).collect::<Vec<_>>())
                            .unwrap_or_else(|| vec![links["ITEM"].as_object().unwrap()])
                            .iter()
                            .for_each(|item| {
                                if let Some(tag_id) = item.get("@ID") {
                                    let mut tags_writer = tags_writer.lock().unwrap();
                                    let _ = tags_writer.write_record(&[
                                        id.clone(),
                                        tag_id
                                            .as_str()
                                            .unwrap_or_default()
                                            .to_string()
                                            .replace("K", ""),
                                    ]);
                                }
                            });
                    }
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
                    let _ = writer.lock().unwrap().write_record(&[
                        data["@ID"].as_str().unwrap_or_default().replace("K", ""),
                        data["NAME_JP"].as_str().unwrap_or_default().to_string(),
                        data["NAME_EN"].as_str().unwrap_or_default().to_string(),
                        data["NAME_R"].as_str().unwrap_or_default().to_string(),
                        array_or_string(&data["NAME_ALT"]),
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
        // tokio::spawn(process_authors()),
        // tokio::spawn(process_contents()),
        // tokio::spawn(process_books()),
        tokio::spawn(process_characters()),
    ];

    future::join_all(tasks).await;

    Ok(())
}
