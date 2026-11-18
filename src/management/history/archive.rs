use gateway_plugin_sdk::{ErrorCode, PluginFault, client::HostClient};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Mutex;

use super::{FULL, MAX_RECORD_BYTES, PAGE_SIZE, Record, invalid, list_entry, state, too_large};

const LEGACY: &str = "pelican-history";
const DATA: &str = "pelican-archive";
const INDEX: &str = "pelican-archive-index";
const HEAD: &str = "head";
const BLOCK: usize = 128;
const MAX_ITEMS: usize = 10_000;
// State values travel in the RPC metadata frame (64 KiB including its envelope).
const ITEM_BYTES: usize = 48 * 1024;
static WRITE: Mutex<()> = Mutex::const_new(());

#[derive(Default, Deserialize, Serialize)]
struct Head {
    sealed: usize,
    tail: Vec<String>,
    pending: Option<Pending>,
}

impl Head {
    fn count(&self) -> usize {
        self.sealed * BLOCK + self.tail.len()
    }

    async fn ids(&self, host: &HostClient, block: usize) -> Result<Vec<String>, PluginFault> {
        if block == self.sealed {
            return Ok(self.tail.clone());
        }
        let (ids, _) = state::get(host, INDEX, &format!("block-{block}")).await?;
        ids.filter(|ids: &Vec<String>| ids.len() == BLOCK)
            .ok_or_else(inconsistent)
    }
}

#[derive(Clone, Deserialize, Serialize)]
struct Pending {
    id: String,
    parts: usize,
}

#[derive(Deserialize, Serialize)]
struct Stored {
    ordinal: usize,
    parts: usize,
    record: Record,
}

pub(in crate::management) async fn append(
    host: &HostClient,
    record: Record,
) -> Result<(), PluginFault> {
    if !record.is_valid() {
        return Err(invalid());
    }
    // One writer in the plugin worker; version checks and CPR's state-owner
    // fence also prevent an obsolete worker from publishing on a version switch.
    let _writer = WRITE.lock().await;
    let (mut head, mut version) = read_head(host).await?;
    if head.pending.is_some() {
        version = Some(recover(host, &mut head, version).await?);
    }
    if published(host, &head, &record.id).await?.is_some()
        || legacy(host).await?.iter().any(|old| old.id == record.id)
    {
        return Ok(());
    }
    if head.count() >= MAX_ITEMS {
        return Err(full());
    }
    let (stored, parts) = pack(record, head.count())?;
    let id = &stored.record.id;
    head.pending = Some(Pending {
        id: id.clone(),
        parts: parts.len(),
    });
    let version = Some(state::put(host, INDEX, HEAD, version, &head).await?);

    // The primary item is written last. Its presence means all source parts
    // are durable, even if a restart happens before the head is published.
    if let Err(error) = write_record(host, &stored, &parts).await {
        if error.code == ErrorCode::Capacity && error.message == "state quota is exhausted" {
            recover(host, &mut head, version).await?;
            return Err(full());
        }
        return Err(error);
    }
    publish(host, &mut head, version, id).await?;
    Ok(())
}

async fn write_record(
    host: &HostClient,
    stored: &Stored,
    parts: &[String],
) -> Result<(), PluginFault> {
    let id = &stored.record.id;
    for (number, part) in parts.iter().enumerate() {
        state::put(host, DATA, &part_key(id, number), None, part).await?;
    }
    state::put(host, DATA, &run_key(id), None, stored).await?;
    Ok(())
}

async fn recover(
    host: &HostClient,
    head: &mut Head,
    version: Option<u64>,
) -> Result<u64, PluginFault> {
    let pending = head.pending.clone().ok_or_else(inconsistent)?;
    if let Some(stored) = read_run(host, &pending.id).await? {
        if stored.ordinal != head.count() || stored.parts != pending.parts {
            return Err(inconsistent());
        }
        return publish(host, head, version, &pending.id).await;
    }
    // These keys belong solely to the unpublished run. Never delete archive
    // items belonging to any run already visible in the index.
    for part in 0..pending.parts {
        let key = part_key(&pending.id, part);
        if let (_, Some(part_version)) = state::get::<String>(host, DATA, &key).await? {
            state::delete(host, DATA, &key, part_version).await?;
        }
    }
    head.pending = None;
    state::put(host, INDEX, HEAD, version, head).await
}

async fn publish(
    host: &HostClient,
    head: &mut Head,
    version: Option<u64>,
    id: &str,
) -> Result<u64, PluginFault> {
    if head.tail.len() == BLOCK {
        let key = format!("block-{}", head.sealed);
        // The sealed block contains only entries already in the head. A
        // reader holding the old head can still use its tail after this write.
        match state::put(host, INDEX, &key, None, &head.tail).await {
            Ok(_) => {}
            Err(error) if error.code == ErrorCode::Conflict => {
                let (sealed, _) = state::get::<Vec<String>>(host, INDEX, &key).await?;
                if sealed.as_ref() != Some(&head.tail) {
                    return Err(inconsistent());
                }
            }
            Err(error) => return Err(error),
        }
        head.sealed += 1;
        head.tail.clear();
    }
    head.tail.push(id.into());
    head.pending = None;
    state::put(host, INDEX, HEAD, version, head).await
}

pub(super) async fn page(
    host: &HostClient,
    offset: usize,
    snapshot: Option<usize>,
) -> Result<(Vec<Value>, usize, usize), PluginFault> {
    let (head, _) = read_head(host).await?;
    let snapshot = snapshot.unwrap_or_else(|| head.count());
    if snapshot > head.count() {
        return Err(invalid());
    }
    let old = legacy(host).await?;
    let total = snapshot + old.len();
    let mut page = Vec::with_capacity(PAGE_SIZE);
    let mut block_number = None;
    let mut ids = Vec::new();
    for index in offset..offset.saturating_add(PAGE_SIZE).min(total) {
        if index < snapshot {
            let ordinal = snapshot - 1 - index;
            let block = ordinal / BLOCK;
            if block_number != Some(block) {
                ids = head.ids(host, block).await?;
                block_number = Some(block);
            }
            let id = ids.get(ordinal % BLOCK).ok_or_else(inconsistent)?;
            let stored = read_run(host, id).await?.ok_or_else(inconsistent)?;
            if stored.ordinal != ordinal {
                return Err(inconsistent());
            }
            page.push(list_entry(stored.record, stored.parts == 0)?);
        } else {
            page.push(list_entry(old[index - snapshot].clone(), true)?);
        }
    }
    Ok((page, total, snapshot))
}

pub(super) async fn detail(host: &HostClient, id: &str) -> Result<Option<Record>, PluginFault> {
    let (head, _) = read_head(host).await?;
    if let Some(mut stored) = published(host, &head, id).await? {
        if stored.parts > 0 {
            let source = stored.record.source.as_mut().ok_or_else(inconsistent)?;
            for number in 0..stored.parts {
                let (part, _) = state::get::<String>(host, DATA, &part_key(id, number)).await?;
                source.push_str(&part.ok_or_else(inconsistent)?);
            }
        }
        return Ok(Some(stored.record));
    }
    Ok(legacy(host)
        .await?
        .into_iter()
        .find(|record| record.id == id))
}

async fn published(
    host: &HostClient,
    head: &Head,
    id: &str,
) -> Result<Option<Stored>, PluginFault> {
    let Some(stored) = read_run(host, id).await? else {
        return Ok(None);
    };
    if stored.ordinal >= head.count() {
        return Ok(None);
    }
    let ids = head.ids(host, stored.ordinal / BLOCK).await?;
    if ids
        .get(stored.ordinal % BLOCK)
        .is_some_and(|saved| saved == id)
    {
        return Ok(Some(stored));
    }
    Err(inconsistent())
}

async fn read_head(host: &HostClient) -> Result<(Head, Option<u64>), PluginFault> {
    let (head, version) = state::get::<Head>(host, INDEX, HEAD).await?;
    Ok((head.unwrap_or_default(), version))
}

async fn legacy(host: &HostClient) -> Result<Vec<Record>, PluginFault> {
    let (records, _) = state::get::<Vec<Record>>(host, LEGACY, "recent").await?;
    Ok(records.unwrap_or_default())
}

async fn read_run(host: &HostClient, id: &str) -> Result<Option<Stored>, PluginFault> {
    let (stored, _) = state::get::<Stored>(host, DATA, &run_key(id)).await?;
    if stored.as_ref().is_some_and(|stored| stored.record.id != id) {
        return Err(inconsistent());
    }
    Ok(stored)
}

fn run_key(id: &str) -> String {
    format!("run-{id}")
}

fn part_key(id: &str, number: usize) -> String {
    format!("run-{id}-part-{number}")
}

fn pack(record: Record, ordinal: usize) -> Result<(Stored, Vec<String>), PluginFault> {
    if serde_json::to_vec(&record).map_err(|_| invalid())?.len() > MAX_RECORD_BYTES {
        return Err(too_large());
    }
    let mut stored = Stored {
        ordinal,
        parts: 0,
        record,
    };
    let mut parts = Vec::new();
    if let Some(source) = stored.record.source.take() {
        // Leave room for the final part count and JSON string delimiters.
        let overhead = serde_json::to_vec(&stored).map_err(|_| invalid())?.len() + 16;
        let budget = ITEM_BYTES.checked_sub(overhead).ok_or_else(too_large)?;
        let (first, mut rest) = source.split_at(json_prefix(&source, budget));
        stored.record.source = Some(first.to_owned());
        while !rest.is_empty() {
            let (part, remaining) = rest.split_at(json_prefix(rest, ITEM_BYTES - 2));
            parts.push(part.to_owned());
            rest = remaining;
        }
        stored.parts = parts.len();
    }
    Ok((stored, parts))
}

fn json_prefix(text: &str, budget: usize) -> usize {
    let mut spent = 0;
    for (index, ch) in text.char_indices() {
        spent += match ch {
            '"' | '\\' | '\n' | '\r' | '\t' | '\u{8}' | '\u{c}' => 2,
            '\0'..='\u{1f}' => 6,
            _ => ch.len_utf8(),
        };
        if spent > budget {
            return index;
        }
    }
    text.len()
}

fn full() -> PluginFault {
    PluginFault::new(ErrorCode::Capacity, FULL)
}

fn inconsistent() -> PluginFault {
    PluginFault::new(ErrorCode::Fault, "pelican history index is inconsistent")
}
