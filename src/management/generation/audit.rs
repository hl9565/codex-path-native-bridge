pub(crate) fn extract_document_slice(source: &str) -> Option<&str> {
    let candidate = extract_fenced_svg_block(source).unwrap_or(source.trim());
    let lower = candidate.to_ascii_lowercase();
    let scan = scan_html(&lower);
    if scan.closed_svgs == 0 {
        return None;
    }
    if let Some(start) = scan.earliest_full_start {
        let end = scan
            .last_html_close_end
            .filter(|end| *end > start)
            .unwrap_or(candidate.len());
        let doc = &candidate[start..end];
        return has_svg(doc).then_some(doc);
    }
    let start = scan.earliest_fragment_start?;
    let end = scan
        .last_fragment_close_end
        .filter(|end| *end > start)
        .unwrap_or(candidate.len());
    let doc = &candidate[start..end];
    has_svg(doc).then_some(doc)
}

pub(crate) fn extract_document_html(source: &str) -> Option<String> {
    let doc = extract_document_slice(source)?;
    let lower = doc.to_ascii_lowercase();
    if lower.starts_with("<!doctype") || starts_with_tag(&lower, "html") {
        Some(doc.to_owned())
    } else {
        Some(format!(
            "<!doctype html><html lang=\"zh-CN\"><head><meta charset=\"utf-8\">\
             <meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"></head>\
             <body style=\"margin:0;display:grid;place-items:center;min-height:100svh;background:#0f172a\">\
             {doc}</body></html>"
        ))
    }
}

pub(crate) fn complete_draft(source: &str) -> bool {
    if source.contains('\0') {
        return false;
    }
    let lower = source.to_ascii_lowercase();
    let scan = scan_html(&lower);
    if !scan.comments_closed
        || !scan.raw_closed
        || scan.closed_svgs == 0
        || scan.svg_depth != 0
        || scan.extra_svg_close
    {
        return false;
    }
    if let Some(html_start) = scan.html_open_pos
        && scan
            .last_html_close_end
            .is_none_or(|close_end| close_end <= html_start)
    {
        return false;
    }
    true
}

pub(crate) fn has_svg(source: &str) -> bool {
    let lower = source.to_ascii_lowercase();
    scan_html(&lower).closed_svgs > 0
}

fn extract_fenced_svg_block(source: &str) -> Option<&str> {
    let mut search = 0usize;
    while let Some(rel) = source[search..].find("```") {
        let open = search + rel + 3;
        let Some(nl) = source[open..].find('\n') else {
            break;
        };
        let lang = source[open..open + nl].trim().to_ascii_lowercase();
        let body_start = open + nl + 1;
        let Some(close_rel) = source[body_start..].find("```") else {
            break;
        };
        let body = source[body_start..body_start + close_rel].trim();
        if (lang.is_empty() || matches!(lang.as_str(), "html" | "htm" | "svg")) && has_svg(body) {
            return Some(body);
        }
        search = body_start + close_rel + 3;
    }
    None
}

struct HtmlScan {
    comments_closed: bool,
    raw_closed: bool,
    earliest_full_start: Option<usize>,
    earliest_fragment_start: Option<usize>,
    last_fragment_close_end: Option<usize>,
    html_open_pos: Option<usize>,
    last_html_close_end: Option<usize>,
    closed_svgs: usize,
    svg_depth: usize,
    extra_svg_close: bool,
}

fn scan_html(lower: &str) -> HtmlScan {
    let bytes = lower.as_bytes();
    let mut scan = HtmlScan {
        comments_closed: true,
        raw_closed: true,
        earliest_full_start: None,
        earliest_fragment_start: None,
        last_fragment_close_end: None,
        html_open_pos: None,
        last_html_close_end: None,
        closed_svgs: 0,
        svg_depth: 0,
        extra_svg_close: false,
    };
    let mut pos = 0usize;
    while pos < bytes.len() {
        let Some(rel_lt) = lower[pos..].find('<') else {
            break;
        };
        let tag_start = pos + rel_lt;
        if lower[tag_start..].starts_with("<!--") {
            if let Some(end_rel) = lower[tag_start + 4..].find("-->") {
                pos = tag_start + 4 + end_rel + 3;
                continue;
            }
            scan.comments_closed = false;
            break;
        }
        let mut handled_raw = false;
        for raw_tag in ["script", "style", "textarea"] {
            if starts_with_tag(&lower[tag_start..], raw_tag) {
                handled_raw = true;
                let Some(open_end) = find_tag_gt(bytes, tag_start) else {
                    scan.raw_closed = false;
                    return scan;
                };
                if scan.svg_depth == 0 && raw_tag == "style" {
                    scan.earliest_fragment_start.get_or_insert(tag_start);
                }
                if is_self_closing(&lower[tag_start..=open_end]) {
                    if scan.svg_depth == 0 {
                        scan.last_fragment_close_end = Some(open_end + 1);
                    }
                    pos = open_end + 1;
                    break;
                }
                let Some(close_start) = find_raw_close(lower, open_end + 1, raw_tag) else {
                    scan.raw_closed = false;
                    return scan;
                };
                let Some(close_end) = find_tag_gt(bytes, close_start) else {
                    scan.raw_closed = false;
                    return scan;
                };
                if scan.svg_depth == 0 {
                    scan.last_fragment_close_end = Some(close_end + 1);
                }
                pos = close_end + 1;
                break;
            }
        }
        if handled_raw {
            continue;
        }
        if lower[tag_start..].starts_with("<!doctype") {
            let rest = &lower[tag_start + "<!doctype".len()..];
            if rest.as_bytes().first().is_some_and(u8::is_ascii_whitespace)
                && rest.trim_start().starts_with("html")
            {
                scan.earliest_full_start.get_or_insert(tag_start);
            }
            pos = find_tag_gt(bytes, tag_start).map_or(bytes.len(), |end| end + 1);
            continue;
        }
        if starts_with_tag(&lower[tag_start..], "html") {
            scan.earliest_full_start.get_or_insert(tag_start);
            scan.html_open_pos.get_or_insert(tag_start);
            pos = find_tag_gt(bytes, tag_start).map_or(bytes.len(), |end| end + 1);
            continue;
        }
        if starts_with_close_tag(&lower[tag_start..], "html") {
            if let Some(close_end) = find_tag_gt(bytes, tag_start) {
                scan.last_html_close_end = Some(close_end + 1);
                pos = close_end + 1;
            } else {
                break;
            }
            continue;
        }
        if starts_with_tag(&lower[tag_start..], "body") {
            if scan.svg_depth == 0 {
                scan.earliest_fragment_start.get_or_insert(tag_start);
            }
            pos = find_tag_gt(bytes, tag_start).map_or(bytes.len(), |end| end + 1);
            continue;
        }
        if starts_with_close_tag(&lower[tag_start..], "body") {
            if let Some(close_end) = find_tag_gt(bytes, tag_start) {
                if scan.svg_depth == 0 {
                    scan.last_fragment_close_end = Some(close_end + 1);
                }
                pos = close_end + 1;
            } else {
                break;
            }
            continue;
        }
        if starts_with_tag(&lower[tag_start..], "svg") {
            let Some(open_end) = find_tag_gt(bytes, tag_start) else {
                break;
            };
            if !is_self_closing(&lower[tag_start..=open_end]) {
                if scan.svg_depth == 0 {
                    scan.earliest_fragment_start.get_or_insert(tag_start);
                }
                scan.svg_depth += 1;
            }
            pos = open_end + 1;
            continue;
        }
        if starts_with_close_tag(&lower[tag_start..], "svg") {
            let Some(close_end) = find_tag_gt(bytes, tag_start) else {
                break;
            };
            if scan.svg_depth > 0 {
                scan.svg_depth -= 1;
                if scan.svg_depth == 0 {
                    scan.closed_svgs += 1;
                    scan.last_fragment_close_end = Some(close_end + 1);
                }
            } else {
                scan.extra_svg_close = true;
            }
            pos = close_end + 1;
            continue;
        }
        let looks_like_tag = bytes
            .get(tag_start + 1)
            .is_some_and(|b| b.is_ascii_alphabetic() || matches!(b, b'/' | b'!' | b'?'));
        pos = if looks_like_tag {
            find_tag_gt(bytes, tag_start).map_or(bytes.len(), |end| end + 1)
        } else {
            tag_start + 1
        };
    }
    scan
}

fn find_raw_close(lower: &str, from: usize, name: &str) -> Option<usize> {
    let needle = format!("</{name}");
    let mut search = from;
    while let Some(rel) = lower[search..].find(&needle) {
        let idx = search + rel;
        let rest = &lower[idx + needle.len()..];
        if rest
            .trim_start_matches(|c: char| c.is_ascii_whitespace())
            .starts_with('>')
        {
            return Some(idx);
        }
        search = idx + needle.len();
    }
    None
}

fn starts_with_tag(slice_lower: &str, name: &str) -> bool {
    slice_lower
        .strip_prefix('<')
        .and_then(|s| s.strip_prefix(name))
        .and_then(|rest| rest.as_bytes().first().copied())
        .is_some_and(|b| b == b'>' || b.is_ascii_whitespace() || b == b'/')
}

fn starts_with_close_tag(slice_lower: &str, name: &str) -> bool {
    slice_lower
        .strip_prefix("</")
        .and_then(|s| s.strip_prefix(name))
        .and_then(|rest| rest.as_bytes().first().copied())
        .is_some_and(|b| b == b'>' || b.is_ascii_whitespace())
}

fn find_tag_gt(bytes: &[u8], start: usize) -> Option<usize> {
    let mut i = start + 1;
    let mut quote: Option<u8> = None;
    while i < bytes.len() {
        let b = bytes[i];
        match quote {
            Some(q) if b == q => quote = None,
            Some(_) => {}
            None if b == b'"' || b == b'\'' => quote = Some(b),
            None if b == b'>' => return Some(i),
            None => {}
        }
        i += 1;
    }
    None
}

fn is_self_closing(open_tag: &str) -> bool {
    open_tag
        .trim_end_matches(|c: char| c.is_ascii_whitespace() || c == '>')
        .ends_with('/')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_draft_rejects_unclosed_script_or_html() {
        assert!(complete_draft(
            "<!doctype html><html><body><svg viewBox='0 0 10 10'><circle/></svg><script>requestAnimationFrame(()=>{})</script></body></html>"
        ));
        assert!(!complete_draft(
            "<!doctype html><html><body><svg viewBox='0 0 10 10'></svg><script>requestAnimationFrame(() =>"
        ));
        assert!(!complete_draft(
            "<!doctype html><html><body><svg viewBox='0 0 10 10'></svg></body>"
        ));
        assert!(complete_draft("<svg viewBox='0 0 10 10'><circle/></svg>"));
    }

    #[test]
    fn extract_document_html_strips_fences_and_wraps_bare_svg() {
        let fenced = "Here is the code:\n```html\n<svg viewBox='0 0 800 450'><rect width='10' height='10'/></svg>\n```";
        let html = extract_document_html(fenced).unwrap();
        assert!(html.starts_with("<!doctype html>"));
        assert!(html.contains("<svg viewBox='0 0 800 450'>"));

        let fragment = "Output:\n<style>.spin{animation:r 2s linear infinite}</style><svg viewBox='0 0 400 200'><circle r='5'/></svg><script>requestAnimationFrame(()=>{})</script>\nDone";
        let wrapped = extract_document_html(fragment).unwrap();
        assert!(wrapped.contains("<style>.spin"));
        assert!(wrapped.contains("<script>requestAnimationFrame"));
        assert!(!wrapped.contains("Done"));
    }

    #[test]
    fn completeness_and_extraction_handle_comments_raw_blocks_and_unicode() {
        assert!(!complete_draft("<svg viewBox='0 0 10 10'><svg viewBox='0 0 5 5'></svg>"));
        assert!(!complete_draft(
            "<!doctype html><html><body><svg viewBox='0 0 10 10'><circle/></svg><script>let end = '</html>';</script></body>"
        ));
        assert!(!complete_draft(
            "<!-- comment without svg close --><svg viewBox='0 0 10 10' />"
        ));
        let extracted = extract_document_html(
            "<!-- preview <svg> --><!doctype html><html><body><svg viewBox='0 0 20 20'><circle r='3'/></svg></body></html>"
        ).unwrap();
        assert!(extracted.starts_with("<!doctype html>"));

        let literal_lt_and_unicode_comment = "<!doctype html><html><head><style>.p::before{content:\"@keyframes x; animation: x 1s\"}</style></head>\
            <body><p>1 < 2</p><svg viewBox='0 0 300 150'><circle r='8'/></svg>\
            <script>// 鹈鹕注释\nfunction tick(){requestAnimationFrame(tick);}</script></body></html>";
        assert!(complete_draft(literal_lt_and_unicode_comment));

        let textarea_svg_only = "<!doctype html><html><body><textarea><svg viewBox='0 0 10 10'><circle/></svg></textarea></body></html>";
        assert!(!has_svg(textarea_svg_only));
        assert!(!complete_draft(textarea_svg_only));
    }
}
