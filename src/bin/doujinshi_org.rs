use glob::glob;
use indicatif::ProgressIterator;

async fn process_books() -> Result<(), Box<dyn std::error::Error>> {
    let mut writer = csv::Writer::from_path("data/doujinshi.org/books.csv")?;
    writer.write_record(&["id", "name", "name_en", "name_romaji", "name_alt"])?;
    for path in glob("data/doujinshi.org/Book/*.json")?
        .filter_map(|entry| entry.ok())
        .collect::<Vec<_>>()
        .iter()
        .progress()
    {
        let file = std::fs::File::open(&path)?;
        let reader = std::io::BufReader::new(file);
        let data: serde_json::Value = serde_json::from_reader(reader)?;
        writer.write_record(&[
            data["@ID"].as_str().unwrap().replace("B", ""),
            data["NAME_JP"].as_str().unwrap_or_default().to_string(),
            data["NAME_EN"].as_str().unwrap_or_default().to_string(),
            data["NAME_R"].as_str().unwrap_or_default().to_string(),
            data["NAME_ALT"].as_str().unwrap_or_default().to_string(),
        ])?;
    }

    writer.flush()?;
    Ok(())
}

async fn process_contents() -> Result<(), Box<dyn std::error::Error>> {
    let mut writer = csv::Writer::from_path("data/doujinshi.org/contents.csv")?;
    writer.write_record(&["id", "name", "name_en", "name_romaji", "name_alt"])?;
    for path in glob("data/doujinshi.org/Content/*.json")?
        .filter_map(|entry| entry.ok())
        .collect::<Vec<_>>()
        .iter()
        .progress()
    {
        let file = std::fs::File::open(&path)?;
        let reader = std::io::BufReader::new(file);
        let data: serde_json::Value = serde_json::from_reader(reader)?;
        writer.write_record(&[
            data["@ID"].as_str().unwrap().replace("C", ""),
            data["NAME_JP"].as_str().unwrap_or_default().to_string(),
            data["NAME_EN"].as_str().unwrap_or_default().to_string(),
            data["NAME_R"].as_str().unwrap_or_default().to_string(),
            data["NAME_ALT"].as_str().unwrap_or_default().to_string(),
        ])?;
    }

    writer.flush()?;
    Ok(())
}

async fn process_authors() -> Result<(), Box<dyn std::error::Error>> {
    let mut writer = csv::Writer::from_path("data/doujinshi.org/authors.csv")?;
    writer.write_record(&["id", "name", "name_en", "name_romaji", "name_alt"])?;
    for path in glob("data/doujinshi.org/Author/*.json")?
        .filter_map(|entry| entry.ok())
        .collect::<Vec<_>>()
        .iter()
        .progress()
    {
        let file = std::fs::File::open(&path)?;
        let reader = std::io::BufReader::new(file);
        let data: serde_json::Value = serde_json::from_reader(reader)?;
        writer.write_record(&[
            data["@ID"].as_str().unwrap().replace("A", ""),
            data["NAME_JP"].as_str().unwrap_or_default().to_string(),
            data["NAME_EN"].as_str().unwrap_or_default().to_string(),
            data["NAME_R"].as_str().unwrap_or_default().to_string(),
            data["NAME_ALT"].as_str().unwrap_or_default().to_string(),
        ])?;
    }

    writer.flush()?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    process_authors().await?;
    process_contents().await?;
    process_books().await?;

    Ok(())
}
