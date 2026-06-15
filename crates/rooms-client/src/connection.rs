use url::Url;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachConfig {
    pub url: String,
    pub room: String,
    pub peer_id: String,
    pub peer_name: Option<String>,
}

pub fn build_attach_url(config: &AttachConfig) -> Result<String, url::ParseError> {
    let mut url = Url::parse(&config.url)?;
    let existing = url
        .query_pairs()
        .filter(|(key, _)| {
            key != "room" && key != "session" && key != "peer_id" && key != "peer_name"
        })
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    let has_replay = existing.iter().any(|(key, _)| key == "replay");

    url.query_pairs_mut().clear().extend_pairs(existing);
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("room", &config.room);
        query.append_pair("peer_id", &config.peer_id);
        if let Some(peer_name) = config
            .peer_name
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
        {
            query.append_pair("peer_name", peer_name);
        }
        if !has_replay {
            query.append_pair("replay", "skip");
        }
    }

    Ok(url.to_string())
}
