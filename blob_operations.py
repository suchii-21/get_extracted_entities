import os
import json
import logging
import base64
import zipfile
import io
import math
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from PyPDF2 import PdfReader
from docx import Document

load_dotenv()

from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from azure.identity import ClientSecretCredential




MAX_PAGES      = 5        # Max allowed pages for PDF / DOCX
WORDS_PER_PAGE = 500      # Proxy ratio used when estimating DOCX page count
MAX_FILE_SIZE  = 15 * 1024 * 1024  # 15 MB in bytes

allowed_content_types = {
    "application/pdf",
    "text/plain",
    "application/zip",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "image/jpeg",
    "image/png",
    "image/heic",
}


DI_PASSTHROUGH_TYPES = {
    "image/jpeg",
    "image/png",
    "image/heic",
    "text/plain",
}

try:
    from document_intelligence_logic import ExtractingContent
    docu_class = ExtractingContent()
except Exception as e:
    logging.error(f"Failure to import the module due to: {e}")
    # raise

EXTENSION_TO_CONTENT_TYPE = {
    ".pdf":  "application/pdf",
    ".txt":  "text/plain",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".jpg":  "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png":  "image/png",
    ".heic": "image/heic",
}

PDF_CONTENT_TYPE  = "application/pdf"
DOCX_CONTENT_TYPE = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"


def _get_pdf_page_count(file_bytes: bytes) -> int:
    try:
        reader = PdfReader(io.BytesIO(file_bytes))
        return len(reader.pages)
    except Exception as e:
        raise ValueError(f"Could not read PDF page count: {e}") from e


def _get_docx_estimated_page_count(file_bytes: bytes) -> int:
    try:
        doc        = Document(io.BytesIO(file_bytes))
        word_count = sum(len(para.text.split()) for para in doc.paragraphs)
        return max(1, math.ceil(word_count / WORDS_PER_PAGE))
    except Exception as e:
        raise ValueError(f"Could not estimate DOCX page count: {e}") from e


def _check_upload_eligibility(
    file_name: str,
    file_size: int,         
    content_type: str,
    file_bytes: bytes,
    result: dict,
    source_label: str = "",
) -> bool:
    """
    Determine whether a file is eligible for blob upload.

    Rules:
      - Images / TXT : always eligible (no size or page check); go straight to DI.
      - PDF / DOCX   : eligible if file_size < MAX_FILE_SIZE AND page_count < MAX_PAGES.
                       Skipped when BOTH conditions fail.

    Returns True  → eligible; caller should proceed with upload + DI.
    Returns False → ineligible; reason already appended to result["skipped"].
    """
    ct        = content_type.lower()
    size_ok   = file_size < MAX_FILE_SIZE
    size_mb   = file_size / (1024 * 1024)

    if ct in DI_PASSTHROUGH_TYPES:
        logging.info(
            f"[ELIGIBILITY] {source_label}'{file_name}' is type '{ct}' "
            f"— skipping size/page checks, sending directly to DI."
        )
        return True

    if ct == PDF_CONTENT_TYPE:
        try:
            pages = _get_pdf_page_count(file_bytes)
        except ValueError as e:
            logging.error(f"[ELIGIBILITY] {source_label}'{file_name}': {e}")
            result["skipped"].append({"file": file_name, "reason": f"pdf read error: {e}"})
            return False

        pages_ok = pages < MAX_PAGES
        logging.info(
            f"[ELIGIBILITY] {source_label}'{file_name}' — "
            f"{pages} page(s), {size_mb:.2f} MB | "
            f"size_ok={size_ok}, pages_ok={pages_ok}"
        )

        if size_ok and pages_ok:
            return True

        logging.warning(
            f"[ELIGIBILITY] {source_label}Skipping '{file_name}' — "
            f"failed validation: {pages} pages (limit: {MAX_PAGES}) "
            f"| {size_mb:.2f} MB (limit: {MAX_FILE_SIZE / (1024*1024):.0f} MB)."
        )
        result["skipped"].append({
            "file":   file_name,
            "reason": (
                f"exceeds limit — "
                f"pages: {pages} (must be < {MAX_PAGES}), "
                f"size: {size_mb:.2f} MB (must be < {MAX_FILE_SIZE / (1024*1024):.0f} MB)"
            ),
        })
        return False

    if ct == DOCX_CONTENT_TYPE:
        try:
            est_pages = _get_docx_estimated_page_count(file_bytes)
        except ValueError as e:
            logging.error(f"[ELIGIBILITY] {source_label}'{file_name}': {e}")
            result["skipped"].append({"file": file_name, "reason": f"docx read error: {e}"})
            return False

        pages_ok = est_pages < MAX_PAGES
        logging.info(
            f"[ELIGIBILITY] {source_label}'{file_name}' — "
            f"~{est_pages} estimated page(s), {size_mb:.2f} MB | "
            f"size_ok={size_ok}, pages_ok={pages_ok}"
        )

        if size_ok and pages_ok:
            return True

        logging.warning(
            f"[ELIGIBILITY] {source_label}Skipping '{file_name}' — "
            f"both checks failed: ~{est_pages} pages >= {MAX_PAGES} "
            f"AND {size_mb:.2f} MB >= {MAX_FILE_SIZE / (1024*1024):.0f} MB."
        )
        result["skipped"].append({
            "file":   file_name,
            "reason": (
                f"exceeds both limits — "
                f"estimated pages: {est_pages} >= {MAX_PAGES}, "
                f"size: {size_mb:.2f} MB >= {MAX_FILE_SIZE / (1024*1024):.0f} MB"
            ),
        })
        return False

    return True


class BlobAttachmentHandler:

    def __init__(self):

        self.keyvault_name = os.getenv('keyvault_url')
        self.kv_uri = f"https://{self.keyvault_name}.vault.azure.net"
        self.credential = ClientSecretCredential(
            tenant_id= os.getenv('AZURE_TENANT_ID'), # type: ignore
            client_id= os.getenv('AZURE_CLIENT_ID'), # type: ignore
            client_secret=os.getenv('AZURE_CLIENT_SECRET') # type: ignore
        )

        self.kv_client = SecretClient(vault_url=self.kv_uri, credential=self.credential)

        self.blob_account_url    = self.get_kv_secrets('BLOB-ACCOUNT-URL')
        self.BLOB_CONTAINER_NAME = self.get_kv_secrets('BLOB-CONTAINER-NAME')

        missing = [k for k, v in {
            "blob_account_url":    self.blob_account_url,
            "BLOB_CONTAINER_NAME": self.BLOB_CONTAINER_NAME,
        }.items() if not v]

        if missing:
            logging.error(f'Missing blob account url')

        try:
            self.blob_service_client = BlobServiceClient(account_url=self.blob_account_url, credential=self.credential)  # type: ignore
            self.container_client = self.blob_service_client.get_container_client(
                self.BLOB_CONTAINER_NAME  # type: ignore
            )
            logging.info("Blob client initialized successfully.")
        except Exception as e:
            logging.error(f"Blob initialization failed: {e}")

    def get_kv_secrets(self, secret_name):
        """
        get keyvault secrets
        """
        try:
            return self.kv_client.get_secret(secret_name).value
        except Exception as e:
            print(f"Error fetching secret {secret_name}: {str(e)}")
            return None

    def _is_allowed_type(self, content_type: str) -> bool:
        return content_type.lower() in allowed_content_types

    def _upload_single_blob(self, email_session_id: str, file_name: str, file_bytes: bytes) -> str:
        blob_path   = f"{email_session_id}/{file_name}"
        blob_client = self.container_client.get_blob_client(blob_path)
        blob_client.upload_blob(file_bytes, overwrite=True)
        logging.info(f"[BLOB UPLOAD] Stored → {blob_path}")
        return blob_path

    def _extract_and_upload_zip(
        self, email_session_id: str, zip_name: str, zip_bytes: bytes, result: dict
    ) -> None:
        logging.info(f"[ZIP] Extracting '{zip_name}' for session '{email_session_id}'.")

        try:
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                for member in zf.infolist():

                    if member.filename.endswith("/"):
                        continue

                    inner_name = os.path.basename(member.filename)
                    if not inner_name:
                        continue

                    ext          = os.path.splitext(inner_name)[1].lower()
                    content_type = EXTENSION_TO_CONTENT_TYPE.get(ext)

                    if content_type is None:
                        logging.warning(
                            f"[ZIP] Skipping '{member.filename}' inside '{zip_name}' — "
                            f"extension '{ext}' not in allowed list."
                        )
                        result["skipped"].append({
                            "file":   f"{zip_name}/{member.filename}",
                            "reason": f"unsupported extension in zip: {ext}",
                        })
                        continue

                    try:
                        inner_bytes = zf.read(member.filename)
                    except Exception as e:
                        logging.error(f"[ZIP] Could not read '{member.filename}': {e}")
                        result["skipped"].append({
                            "file":   f"{zip_name}/{member.filename}",
                            "reason": f"zip read error: {e}",
                        })
                        continue

                    
                    inner_file_size = len(inner_bytes)
                    #Extract again if zip folder is available inside the zip
                    if content_type == "application/zip":
                        self._extract_and_upload_zip(
                            email_session_id, inner_name, inner_bytes, result
                        )
                        continue

                    
                    if content_type.lower() in DI_PASSTHROUGH_TYPES:
                        logging.info(
                            f"[ZIP] '{inner_name}' is passthrough type "
                            f"— sending to DI without blob upload."
                        )
                        extracted = self._call_document_intelligence(email_session_id, inner_bytes, inner_name)
                        if extracted:
                            result["extracted_contents"][inner_name] = extracted
                        continue

                    if not _check_upload_eligibility(
                        inner_name, inner_file_size, content_type, inner_bytes, result,
                        source_label=f"ZIP: {zip_name} → "
                    ):
                        continue

                    try:
                        blob_path = self._upload_single_blob(
                            email_session_id, inner_name, inner_bytes
                        )
                        result["uploaded"].append(blob_path)
                        extracted = self._call_document_intelligence(email_session_id, inner_bytes, inner_name)
                        if extracted:
                            result["extracted_contents"][inner_name] = extracted
                        logging.info(
                            f"[ZIP] Uploaded '{inner_name}' "
                            f"(extracted from '{zip_name}') → {blob_path}"
                        )
                    except Exception as e:
                        logging.error(
                            f"[ZIP] Failed to upload '{inner_name}' from '{zip_name}': {e}"
                        )
                        result["skipped"].append({
                            "file":   f"{zip_name}/{inner_name}",
                            "reason": f"upload error: {e}",
                        })

        except zipfile.BadZipFile as e:
            logging.error(f"[ZIP] '{zip_name}' is not a valid ZIP file: {e}")
            result["skipped"].append({"file": zip_name, "reason": f"bad zip: {e}"})
        except Exception as e:
            logging.error(f"[ZIP] Unexpected error processing '{zip_name}': {e}")
            result["skipped"].append({"file": zip_name, "reason": f"zip error: {e}"})

    def uploading_attachments_to_blob(
        self, email_session_id: str, attachments_raw: list
    ) -> dict:
        result = {
            "status":             "no_attachments",
            "uploaded":           [],
            "skipped":            [],
            "loaded":             [],
            "extracted_contents": {},
        }

        if attachments_raw:
            logging.info(f"Processing attachments for email session '{email_session_id}'.")

            for attachment in attachments_raw:
                file_name     = attachment.get("name", "unknown_file")
                content_bytes = attachment.get("contentBytes", "")
                file_size     = attachment.get("size", 0)   # incoming size from the payload
                content_type  = attachment.get("contentType", "application/octet-stream")
                is_inline     = attachment.get("isInline", False)

                if is_inline:
                    logging.info(f"[ATTACHMENT] Skipping inline attachment: {file_name}")
                    result["skipped"].append({"file": file_name, "reason": "inline"})
                    continue

                if not self._is_allowed_type(content_type): # if file is not allowed store
                    logging.warning(
                        f"[ATTACHMENT] Skipping '{file_name}' — "
                        f"unsupported content type '{content_type}'."
                    )
                    result["skipped"].append({
                        "file":   file_name,
                        "reason": f"unsupported type: {content_type}",
                    })
                    continue

                try:
                    file_bytes = base64.b64decode(content_bytes)
                except Exception as e:
                    logging.warning(f"[ATTACHMENT] Could not base64-decode '{file_name}': {e}")
                    result["skipped"].append({"file": file_name, "reason": f"decode error: {e}"})
                    continue

                # Fall back to actual byte length if the payload size is missing/zero
                if not file_size:
                    file_size = len(file_bytes)

                if content_type.lower() == "application/zip":
                    self._extract_and_upload_zip(
                        email_session_id, file_name, file_bytes, result
                    )
                    continue

                if content_type.lower() in DI_PASSTHROUGH_TYPES:
                    logging.info(
                        f"[ATTACHMENT] '{file_name}' is passthrough type "
                        f"— sending directly to DI, skipping blob upload."
                    )
                    extracted = self._call_document_intelligence(email_session_id, file_bytes, file_name)
                    if extracted:
                        result["extracted_contents"][file_name] = extracted
                    continue

                if not _check_upload_eligibility(file_name, file_size, content_type, file_bytes, result):
                    continue

                try:
                    blob_path = self._upload_single_blob(email_session_id, file_name, file_bytes)
                    result["uploaded"].append(blob_path)
                    extracted = self._call_document_intelligence(email_session_id, file_bytes, file_name)
                    if extracted:
                        result["extracted_contents"][file_name] = extracted
                except Exception as e:
                    logging.error(f"[BLOB UPLOAD] Failed to upload '{file_name}': {e}")
                    result["skipped"].append({"file": file_name, "reason": f"upload error: {e}"})

            if result["uploaded"]:
                result["status"] = "uploaded"
            else:
                result["status"] = "no_attachments"

        if not attachments_raw:
            return result

        else:
            prefix = f"{email_session_id}/"
            logging.info(
                f"[BLOB SCAN] No attachments in payload. "
                f"Scanning blob storage for prefix '{prefix}'."
            )

            blob_list = list(self.container_client.list_blobs(name_starts_with=prefix))
            logging.info(f"[BLOB COUNT] Found {len(blob_list)} existing blob(s).")

            for blob in blob_list:
                logging.info(f"[BLOB LOAD] Loading existing blob: {blob.name}")
                try:
                    blob_client = self.container_client.get_blob_client(blob.name)
                    file_bytes  = bytes(blob_client.download_blob().readall())
                    result["loaded"].append({
                        "fileName":  blob.name.split("/")[-1],
                        "fileBytes": file_bytes,
                    })
                except Exception as e:
                    logging.error(f"[BLOB LOAD] Failed to load '{blob.name}': {e}")

            if result["loaded"]:
                result["status"] = "scanned"
            else:
                logging.warning(f"[BLOB SCAN] No blobs found for session '{email_session_id}'.")

        logging.info(
            f"[SUMMARY] session='{email_session_id}' "
            f"uploaded={len(result['uploaded'])} "
            f"skipped={len(result['skipped'])} "
            f"loaded={len(result['loaded'])} "
            f"extracted={len(result['extracted_contents'])}"
        )
        return result

    def upload_email_body(self, txt_name, email_session_id):
        """
        uploading the email body as a .txt to the blob
        """
        local_file = txt_name
        blob_path  = f"{email_session_id}/{local_file}"

        try:
            with open(local_file, "rb") as f:
                file_bytes = f.read()

            blob_client = self.container_client.get_blob_client(blob_path)
            blob_client.upload_blob(file_bytes, overwrite=True)
            logging.info(f"[EMAIL BODY] Uploaded '{local_file}' → '{blob_path}'")

        except FileNotFoundError:
            logging.error(
                f"[EMAIL BODY] '{local_file}' not found locally — "
                f"was append_to_txt() called before this?"
            )
        except Exception as e:
            logging.error(f"[EMAIL BODY] Failed to upload email body to blob: {e}")

    def upload_extracted_content(self, email_session_id: str) -> None:

        local_file = f"{email_session_id[:4]}content_json.json"
        blob_path  = f"{email_session_id}/{local_file}"

        try:
            with open(local_file, "rb") as f:
                file_bytes = f.read()

            blob_client = self.container_client.get_blob_client(blob_path)
            blob_client.upload_blob(file_bytes, overwrite=True)
            logging.info(f"[EXTRACTED CONTENT] Uploaded '{local_file}' → '{blob_path}'")

        except FileNotFoundError:
            logging.error(
                f"[EXTRACTED CONTENT] '{local_file}' not found — "
                f"was write_to_json() called before this?"
            )
        except Exception as e:
            logging.error(f"[EXTRACTED CONTENT] Failed to upload content JSON: {e}")

    def _call_document_intelligence(self, email_session_id, file_bytes: bytes, file_name: str) -> str | None:
        """
        Call Document Intelligence and return the extracted content string.
        Logs errors but never raises.
        """
        try:
            return docu_class.extract_content(email_session_id, file_bytes, file_name, self)
        except Exception as e:
            logging.error(f"[DI] Document Intelligence failed for '{file_name}': {e}")
            return None