use std::{path::PathBuf, sync::LazyLock};

use anyhow::Result;
use futures::future;
use glob::glob;
use indicatif::{ParallelProgressIterator, ProgressIterator};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

static DATA_DIR: LazyLock<PathBuf> = LazyLock::new(|| PathBuf::from("data/doujinshi.org"));

async fn process<F>(pattern: &str, output_file: &str, header: Vec<&str>, transform: F) -> Result<()>
where
    F: Fn(&serde_json::Value) -> Vec<String> + Send + Sync + 'static,
{
    let mut writer = csv::Writer::from_path(output_file)?;
    writer.write_record(&header)?;

    glob(pattern)?
        .filter_map(Result::ok)
        .collect::<Vec<_>>()
        .par_iter()
        .progress()
        .map(|file| {
            let file = std::fs::File::open(file).unwrap();
            let reader = std::io::BufReader::new(file);
            let json: serde_json::Value = serde_json::from_reader(reader).unwrap();
            transform(&json)
        })
        .collect::<Vec<_>>()
        .into_iter()
        .progress()
        .for_each(|record| {
            writer.write_record(&record).unwrap();
        });

    writer.flush()?;

    Ok(())
}

async fn process_authors() {
    process(
        &DATA_DIR.join("Author").join("*.json").to_string_lossy(),
        &DATA_DIR.join("authors.csv").to_string_lossy(),
        vec!["id", "name", "name_en", "name_romaji", "name_alt"],
        |json| {
            vec![
                json["@ID"].as_str().unwrap_or_default().replace("A", ""),
                json["NAME_JP"].as_str().unwrap_or_default().to_string(),
                json["NAME_EN"].as_str().unwrap_or_default().to_string(),
                json["NAME_R"].as_str().unwrap_or_default().to_string(),
                json["NAME_ALT"]
                    .as_array()
                    .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>())
                    .unwrap_or_else(|| vec![json["NAME_ALT"].as_str().unwrap_or_default()])
                    .join(","),
            ]
        },
    )
    .await
    .unwrap()
}

#[tokio::main]
async fn main() {
    future::join_all(vec![tokio::spawn(process_authors())]).await;

    println!("Processing completed.")
}
