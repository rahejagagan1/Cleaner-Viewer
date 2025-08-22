# runpod_storage_cleanup_with_feedback_viewer.py

import os
import json
import socket
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List, Tuple

import streamlit as st
import pandas as pd
from dotenv import load_dotenv
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

# ================ Secrets loader ================
load_dotenv()

def _sget(key: str, default: Optional[str] = None, section: Optional[str] = None) -> Optional[str]:
    v = os.getenv(key)
    if v not in (None, ""):
        return v
    try:
        if section:
            sect = st.secrets.get(section, {})
            if isinstance(sect, dict) and key in sect and sect[key]:
                return sect[key]
        v2 = st.secrets.get(key, default)
        return v2 if v2 not in ("", None) else default
    except Exception:
        return default

def _require(name: str, value: Optional[str]):
    if not value:
        st.error(f"Missing secret: **{name}**")
        st.stop()

S3_REGION = _sget("RUNPOD_S3_REGION", section="runpod_s3") or _sget("RUNPOD_S3_REGION")
S3_BUCKET = _sget("RUNPOD_S3_BUCKET", section="runpod_s3") or _sget("RUNPOD_S3_BUCKET")
S3_KEY    = _sget("RUNPOD_S3_ACCESS_KEY", section="runpod_s3") or _sget("RUNPOD_S3_ACCESS_KEY")
S3_SECRET = _sget("RUNPOD_S3_SECRET_KEY", section="runpod_s3") or _sget("RUNPOD_S3_SECRET_KEY")

for k, v in {
    "RUNPOD_S3_REGION": S3_REGION,
    "RUNPOD_S3_BUCKET": S3_BUCKET,
    "RUNPOD_S3_ACCESS_KEY": S3_KEY,
    "RUNPOD_S3_SECRET_KEY": S3_SECRET,
}.items():
    _require(k, v)

# ========================= Client + helpers =========================
def _dns_ok(host: str) -> bool:
    try:
        socket.gethostbyname(host)
        return True
    except socket.gaierror:
        return False

def _endpoint(region: str) -> str:
    return f"https://s3api-{region}.runpod.io"

ENDPOINT = _endpoint(S3_REGION)
if not _dns_ok(ENDPOINT.replace("https://", "")):
    st.error(f"DNS cannot resolve {ENDPOINT}. Check DNS/network.")
    st.stop()

s3 = boto3.client(
    "s3",
    region_name=S3_REGION,
    endpoint_url=ENDPOINT,
    aws_access_key_id=S3_KEY,
    aws_secret_access_key=S3_SECRET,
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
)

def _human(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    s = float(n)
    for u in units:
        if s < 1024 or u == units[-1]:
            return f"{s:.2f} {u}"
        s /= 1024

def _safe_rerun():
    try:
        st.rerun()
    except AttributeError:
        st.experimental_rerun()

def list_objects(prefix: str, limit: int = 200000) -> List[Dict[str, Any]]:
    """Return list of dicts: Key, Size, LastModified (datetime)"""
    out: List[Dict[str, Any]] = []
    paginator = s3.get_paginator("list_objects_v2")
    try:
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith("/"):
                    continue
                out.append({
                    "Key": key,
                    "Size": int(obj.get("Size", 0)),
                    "LastModified": obj.get("LastModified"),
                })
                if len(out) >= limit:
                    return out
    except ClientError as e:
        st.error(f"Error listing {prefix}: {e}")
    return out

# ===== Selection helpers (no callbacks) =====
def _ensure_select_state(sec: str, n_rows: int, default: bool = False):
    """Ensure st.session_state[f'select_col_{sec}'] exists and matches n_rows."""
    key = f"select_col_{sec}"
    if key not in st.session_state:
        st.session_state[key] = [default] * n_rows
    else:
        prev = st.session_state[key]
        if len(prev) != n_rows:
            if len(prev) < n_rows:
                prev = prev + [default] * (n_rows - len(prev))
            else:
                prev = prev[:n_rows]
            st.session_state[key] = prev

# --------- Robust deletion with detailed errors ----------
def delete_keys_verbose(
    keys: List[str],
    *,
    ignore_no_such: bool = True,
    per_key_retry: bool = True
) -> Tuple[int, List[Dict[str, str]]]:
    deleted = 0
    errors: List[Dict[str, str]] = []
    CHUNK = 1000

    for i in range(0, len(keys), CHUNK):
        chunk = keys[i:i+CHUNK]
        try:
            resp = s3.delete_objects(
                Bucket=S3_BUCKET,
                Delete={"Objects": [{"Key": k} for k in chunk], "Quiet": True}
            )
        except ClientError as e:
            if per_key_retry:
                for k in chunk:
                    try:
                        s3.delete_object(Bucket=S3_BUCKET, Key=k)
                        deleted += 1
                    except ClientError as e1:
                        err_code = getattr(e1, "response", {}).get("Error", {}).get("Code", "ClientError")
                        err_msg  = getattr(e1, "response", {}).get("Error", {}).get("Message", str(e1))
                        if ignore_no_such and err_code in ("NoSuchKey", "404", "NotFound"):
                            deleted += 1
                        else:
                            errors.append({"key": k, "code": err_code, "message": err_msg})
            else:
                errors.append({"key": f"{len(chunk)} keys", "code": "BulkDeleteFailed", "message": str(e)})
            continue

        deleted += len(resp.get("Deleted", []))

        for err in resp.get("Errors", []):
            k    = err.get("Key", "")
            code = err.get("Code", "")
            msg  = err.get("Message", "")

            if ignore_no_such and code in ("NoSuchKey", "404", "NotFound"):
                deleted += 1
                continue

            if per_key_retry:
                try:
                    s3.delete_object(Bucket=S3_BUCKET, Key=k)
                    deleted += 1
                    continue
                except ClientError as e2:
                    code2 = getattr(e2, "response", {}).get("Error", {}).get("Code", code or "ClientError")
                    msg2  = getattr(e2, "response", {}).get("Error", {}).get("Message", msg or str(e2))
                    errors.append({"key": k, "code": code2, "message": msg2})
            else:
                errors.append({"key": k, "code": code, "message": msg})
    return deleted, errors

# --------- Versioned bucket helpers ----------
def bucket_versioning_status() -> str:
    try:
        resp = s3.get_bucket_versioning(Bucket=S3_BUCKET)
        return resp.get("Status", "") or ""
    except ClientError:
        return ""

def delete_all_versions(keys: List[str]) -> Tuple[int, List[Dict[str, str]]]:
    deleted = 0
    errors: List[Dict[str, str]] = []
    CHUNK = 1000

    objs_with_versions = []
    for k in keys:
        try:
            paginator = s3.get_paginator("list_object_versions")
            for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=k):
                for v in page.get("Versions", []):
                    if v.get("Key") == k and "VersionId" in v:
                        objs_with_versions.append({"Key": k, "VersionId": v["VersionId"]})
                for dm in page.get("DeleteMarkers", []):
                    if dm.get("Key") == k and "VersionId" in dm:
                        objs_with_versions.append({"Key": k, "VersionId": dm["VersionId"]})
        except ClientError as e:
            errors.append({"key": k, "code": "ListObjectVersionsError", "message": str(e)})

    for i in range(0, len(objs_with_versions), CHUNK):
        chunk = objs_with_versions[i:i+CHUNK]
        try:
            resp = s3.delete_objects(
                Bucket=S3_BUCKET,
                Delete={"Objects": chunk, "Quiet": True}
            )
            deleted += len(resp.get("Deleted", []))
            for err in resp.get("Errors", []):
                errors.append({"key": err.get("Key",""), "code": err.get("Code",""), "message": err.get("Message","")})
        except ClientError as e:
            errors.append({"key": f"{len(chunk)} entries", "code": "VersionedBulkDeleteFailed", "message": str(e)})

    return deleted, errors

def list_all_under_prefix(prefix: str) -> List[str]:
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix if prefix.endswith("/") else prefix + "/"):
        for obj in page.get("Contents", []):
            k = obj["Key"]
            if not k.endswith("/"):
                keys.append(k)
    return keys

# ========================= NEW: Feedback helpers =========================
def _safe_json_load(b: bytes) -> Dict[str, Any]:
    try:
        return json.loads(b.decode("utf-8"))
    except Exception:
        try:
            return json.loads(b)
        except Exception:
            return {}

def _feedback_prefix(section: str) -> Optional[str]:
    if section == "feedback":
        return "feedback/"
    if section == "Srt-model/feedback":
        return "Srt-model/feedback/"
    return None

def _is_feedback_section(section: str) -> bool:
    return section in ("feedback", "Srt-model/feedback")

def _fetch_feedback_record(key: str) -> Optional[Dict[str, Any]]:
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        body = obj["Body"].read()
        data = _safe_json_load(body)
        return {
            "s3_key": key,
            "filename": data.get("filename", ""),
            "username": data.get("username", ""),
            "feedback": data.get("feedback", ""),
            "job_id": data.get("job_id", ""),
            "created_at": data.get("created_at", obj.get("LastModified").isoformat() if obj.get("LastModified") else None),
        }
    except ClientError as e:
        st.warning(f"Could not read {key}: {e}")
    except Exception as e:
        st.warning(f"Failed parsing {key}: {e}")
    return None

@st.cache_data(ttl=60, show_spinner=False)
def _load_feedback_df(prefix: str) -> pd.DataFrame:
    keys: List[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    try:
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith("/"):
                    continue
                keys.append(key)
    except ClientError as e:
        st.error(f"Error listing feedback: {e}")
        return pd.DataFrame(columns=["filename","username","feedback","job_id","created_at","s3_key"])

    rows = []
    for k in keys:
        rec = _fetch_feedback_record(k)
        if rec:
            rows.append(rec)

    if not rows:
        return pd.DataFrame(columns=["filename","username","feedback","job_id","created_at","s3_key"])

    df = pd.DataFrame(rows)
    if "created_at" in df.columns:
        df["created_at_parsed"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True)
    return df

# ========================= UI =========================
st.set_page_config(
    page_title="RunPod Storage Cleanup",
    page_icon="🧹",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.title("🧹 RunPod Storage Cleanup")
st.caption(f"Bucket: `{S3_BUCKET}` • Region: `{S3_REGION}` • Endpoint: `{ENDPOINT}`")

st.header("Browse")
col1, col2 = st.columns(2)
with col1:
    section = st.selectbox(
        "Select section",
        ["uploads", "transcriptions", "feedback", "Srt-model/uploads", "Srt-model/transcriptions", "Srt-model/feedback"]
    )
with col2:
    confirm_phrase = st.text_input("Type DELETE to confirm", value="", placeholder="DELETE")

st.divider()

with st.sidebar:
    st.header("Filters")
    contains = st.text_input("Name contains", value="")
    older_than_days = st.number_input("Older than (days)", min_value=0, max_value=3650, value=0, step=1)
    apply_filter = st.button("Apply filters")
    st.divider()
    st.header("Danger zone")
    dry_run = st.checkbox("Dry run (preview only)", value=False)
    delete_versions_too = st.checkbox("Delete all versions (if bucket is versioned)", value=False)
    show_raw_errors = st.checkbox("Show raw error details", value=True)

ver_status = bucket_versioning_status()
st.caption(f"Bucket versioning: **{ver_status or 'Unknown/Disabled'}**")

prefix = f"{section}/"
now = datetime.now(timezone.utc)
min_date = now - timedelta(days=int(older_than_days)) if older_than_days > 0 else None

def _match_name(key: str, needle: str) -> bool:
    return needle.lower() in key.lower()

# -------------------- Layout: Tabs for Feedback Sections --------------------
if _is_feedback_section(section):
    tab1, tab2 = st.tabs(["🗂 Browse / Delete", "💬 View Feedback"])
else:
    tab1 = st.container()
    tab2 = None

# -------------------- TAB 1: Browse/Delete --------------------
with tab1:
    with st.spinner(f"Listing {prefix}…"):
        objects = list_objects(prefix)

    # Build filtered rows (common for all sections)
    rows = []
    for o in objects:
        k = o["Key"]
        if contains and not _match_name(k, contains):
            continue
        if min_date and o["LastModified"] and o["LastModified"] > min_date:
            continue
        rows.append({
            "key": k,
            "size": o["Size"],
            "size_readable": _human(int(o["Size"])),
            "last_modified": o["LastModified"],
        })

    df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["key","size","size_readable","last_modified"])

    st.subheader(f"{section.capitalize()} files")
    st.caption(f"{len(df)} items matched")

    if not df.empty:
        df = df.sort_values(by="last_modified", ascending=False).reset_index(drop=True)

        st.write("Select files to delete:")

        # Build the displayed frame first
        display_df = df.copy()
        display_df = display_df[["key", "size_readable", "last_modified", "size"]]

        # Ensure selection list exists and matches current row count
        _ensure_select_state(section, len(display_df), default=False)

        # Derive current "all selected" state to show an accurate checkbox default
        all_selected_now = len(display_df) > 0 and all(st.session_state[f"select_col_{section}"])

        # Plain checkbox (no callback)
        selall_key = f"selall_{section}"
        selall_prev_key = f"__selall_prev_{section}"

        sel_all = st.checkbox("Select all shown", key=selall_key, value=all_selected_now)

        # If the checkbox value changed since last run, rewrite the selection list
        prev_val = st.session_state.get(selall_prev_key, None)
        if prev_val is None or prev_val != sel_all:
            st.session_state[selall_prev_key] = sel_all
            st.session_state[f"select_col_{section}"] = [bool(sel_all)] * len(display_df)

        # Insert current selection column from session state
        display_df.insert(0, "select", st.session_state[f"select_col_{section}"])

        # Render editable table
        edited_df = st.data_editor(
            display_df,
            hide_index=True,
            column_config={
                "select": st.column_config.CheckboxColumn("Select", help="Mark rows to delete"),
                "key": st.column_config.TextColumn("Key"),
                "size_readable": st.column_config.TextColumn("Size"),
                "last_modified": st.column_config.DatetimeColumn("Last modified"),
                "size": st.column_config.NumberColumn("Size (bytes)", help="Raw size in bytes", format="%.0f"),
            },
            disabled=["key", "size_readable", "last_modified", "size"],
            key=f"data_editor_{section}",
        )

        # Persist row-by-row selection back to session state every run
        st.session_state[f"select_col_{section}"] = (
            edited_df["select"].fillna(False).astype(bool).tolist()
        )

        # Compute selected keys and total size
        selected_mask = edited_df["select"].fillna(False).astype(bool)
        selected_keys: List[str] = edited_df.loc[selected_mask, "key"].tolist()
        total_selected_size = int(edited_df.loc[selected_mask, "size"].sum()) if selected_keys else 0

        st.divider()
        st.markdown(
            f"**Selected:** {len(selected_keys)} files • Total size: "
            f"{_human(total_selected_size) if selected_keys else '0 B'}"
        )

        bcol1, bcol2 = st.columns([1, 1])
        with bcol1:
            delete_clicked = st.button("🧨 Delete selected", type="primary", disabled=not selected_keys)
        with bcol2:
            refresh_clicked = st.button("🔄 Refresh page")

        if refresh_clicked:
            _safe_rerun()

        if delete_clicked:
            if confirm_phrase.strip().upper() != "DELETE":
                st.error("Type DELETE in the confirmation box to proceed.")
            else:
                if dry_run:
                    st.info(f"[Dry run] Would delete {len(selected_keys)} objects.")
                else:
                    with st.spinner("Deleting…"):
                        del_count, errs = delete_keys_verbose(
                            selected_keys,
                            ignore_no_such=True,
                            per_key_retry=True
                        )
                        if delete_versions_too and ver_status == "Enabled":
                            vdel, verrs = delete_all_versions(selected_keys)
                            del_count += vdel
                            errs.extend(verrs)

                    st.success(f"Deleted {del_count} objects (treating 'NoSuchKey' as already deleted).")
                    if errs:
                        st.warning(f"{len(errs)} errors encountered.")
                        if show_raw_errors:
                            for e in errs[:200]:
                                st.write(f"• **{e.get('key','')}** — `{e.get('code','')}` — {e.get('message','')}")
                    _safe_rerun()
    else:
        st.info("No matching items.")

# -------------------- TAB 2: Feedback Viewer (only for feedback sections) --------------------
if tab2 is not None:
    with tab2:
        fb_prefix = _feedback_prefix(section)
        if not fb_prefix:
            st.info("Select a feedback section to view contents.")
        else:
            st.subheader("Feedback Viewer")
            with st.spinner("Loading feedback JSON…"):
                df_fb = _load_feedback_df(fb_prefix)

            if df_fb.empty:
                st.info(f"No feedback JSON found under `{fb_prefix}`.")
            else:
                # Sidebar controls for viewer
                with st.sidebar:
                    st.header("Feedback filters")
                    f_filename = st.text_input("Filename contains (viewer)", "")
                    f_user = st.text_input("Username contains (viewer)", "")
                    f_text = st.text_input("Feedback contains (viewer)", "")
                    sort_by = st.selectbox("Sort by (viewer)", ["created_at_parsed", "filename", "username"])
                    sort_asc = st.checkbox("Ascending (viewer)", value=False)
                    max_rows = st.number_input("Max rows (viewer)", min_value=50, max_value=5000, value=1000, step=50)
                    viewer_refresh = st.button("🔄 Refresh viewer")

                # Apply filters
                view = df_fb.copy()
                if f_filename.strip():
                    view = view[view["filename"].fillna("").str.contains(f_filename.strip(), case=False, na=False)]
                if f_user.strip():
                    view = view[view["username"].fillna("").str.contains(f_user.strip(), case=False, na=False)]
                if f_text.strip():
                    view = view[view["feedback"].fillna("").str.contains(f_text.strip(), case=False, na=False)]

                # Sort + limit
                if sort_by in view.columns:
                    view = view.sort_values(by=sort_by, ascending=sort_asc, na_position="last")
                view = view.head(int(max_rows))

                st.caption(f"Showing {len(view)} of {len(df_fb)} rows after filters")
                show_cols = ["created_at_parsed", "filename", "username", "feedback", "job_id", "s3_key"]
                present_cols = [c for c in show_cols if c in view.columns]

                st.markdown("### Table")
                st.dataframe(
                    view[present_cols].rename(columns={"created_at_parsed": "created_at (UTC)"}),
                    use_container_width=True,
                    hide_index=True,
                )

                st.markdown("### Details")
                for _, row in view.iterrows():
                    with st.expander(f"🗂 {row.get('filename','')} — 👤 {row.get('username','')} — 🕒 {row.get('created_at_parsed','')}"):
                        st.write("**S3 key:**", row.get("s3_key", ""))
                        st.write("**Feedback:**")
                        st.code(row.get("feedback", ""), language="text")
                        try:
                            obj = s3.get_object(Bucket=S3_BUCKET, Key=row.get("s3_key"))
                            raw = obj["Body"].read().decode("utf-8", errors="ignore")
                            # st.write("**Raw JSON:**")
                            # st.code(raw, language="json")
                        except Exception as e:
                            st.warning(f"Could not fetch raw JSON: {e}")

                # Export (viewer)
                st.markdown("### Export ")
                export_cols = ["created_at_parsed", "filename", "username", "feedback", "job_id", "s3_key"]
                present_export = [c for c in export_cols if c in view.columns]
                export_df = view[present_export].copy()

                if "created_at_parsed" in export_df.columns:
                    export_df = export_df.rename(columns={"created_at_parsed": "created_at_utc"})
                    export_df["created_at_utc"] = pd.to_datetime(
                        export_df["created_at_utc"], errors="coerce", utc=True
                    ).dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

                csv_bytes = export_df.to_csv(index=False).encode("utf-8")
                st.download_button("⬇️ Download CSV", data=csv_bytes, file_name="feedback_export.csv", mime="text/csv")

                if st.button("🔄 Refresh Page"):
                    _load_feedback_df.clear()
                    _safe_rerun()
                
                if viewer_refresh:
                    _load_feedback_df.clear()
                    _safe_rerun()
