# This file is modified to store files locally instead of uploading to Telegram
# Files are saved to /data/shared with progress feedback

from asyncio import sleep
from logging import getLogger
from os import path as ospath, walk, makedirs
from re import match as re_match, sub as re_sub
from time import time

from aioshutil import rmtree, copy as async_copy
from natsort import natsorted

from aiofiles.os import (
    path as aiopath,
    remove,
    rename,
)
from bot.core.config_manager import Config
from bot.helper.ext_utils.bot_utils import sync_to_async
from bot.helper.ext_utils.files_utils import check_strict_file_mode, get_base_name, is_archive
from bot.helper.ext_utils.status_utils import get_readable_file_size, get_readable_time
from bot.helper.telegram_helper.message_utils import send_message
from bot.helper.ext_utils.media_utils import (
    get_audio_thumbnail,
    get_document_type,
    get_media_info,
    get_video_thumbnail,
    get_md5_hash,
)
from bot.helper.telegram_helper.message_utils import delete_message

LOGGER = getLogger(__name__)

# Storage path
STORAGE_PATH = "/data/shared"


class CancelledUpload(BaseException):
    pass


class TelegramUploader:
    def __init__(self, listener, path):
        self._last_uploaded = 0
        self._processed_bytes = 0
        self._listener = listener
        self._path = path
        self._start_time = time()
        self._total_files = 0
        self._thumb = self._listener.thumb or f"thumbnails/{listener.user_id}.jpg"
        self._msgs_dict = {}
        self._corrupted = 0
        self._is_corrupted = False
        self._up_path = ""
        self._lprefix = ""
        self._lsuffix = ""
        self._lcaption = ""
        self._lfont = ""
        self._sent_msg = None
        self._log_msg = None
        self._error = ""
        self._auto_thumb_enabled = (
            not self._listener.thumb and
            self._listener.user_dict.get("AUTO_THUMBNAIL", False)
        )
        self._auto_thumb_path = None
        self._status_message = None

    def _check_cancelled(self):
        if self._listener.is_cancelled:
            raise CancelledUpload()

    async def _simulate_progress(self, current, total):
        """Simulate upload progress feedback"""
        if self._listener.is_cancelled:
            raise CancelledUpload()
        chunk_size = current - self._last_uploaded
        self._last_uploaded = current
        self._processed_bytes += chunk_size
        
        # Update progress message every 0.5 seconds
        if self._status_message and (time() - getattr(self, '_last_progress_update', 0) >= 0.5):
            self._last_progress_update = time()
            try:
                progress_text = self._get_progress_text()
                await self._status_message.edit_text(progress_text)
            except Exception:
                pass

    def _get_progress_text(self):
        """Generate progress text similar to Telegram upload"""
        elapsed = time() - self._start_time
        speed = self._processed_bytes / elapsed if elapsed > 0 else 0
        speed_str = get_readable_file_size(speed)
        processed_str = get_readable_file_size(self._processed_bytes)
        
        return (
            f"📤 **Storing files to VPS...**\n\n"
            f"**Processed:** {processed_str}\n"
            f"**Speed:** {speed_str}/s\n"
            f"**Files:** {self._total_files}\n"
            f"**Elapsed:** {get_readable_time(elapsed)}"
        )

    async def _user_settings(self):
        settings_map = {
            "BOT_PM": ("_bot_pm", False),
            "LEECH_PREFIX": ("_lprefix", ""),
            "LEECH_SUFFIX": ("_lsuffix", ""),
            "LEECH_CAPTION": ("_lcaption", ""),
            "LEECH_FONT": ("_lfont", ""),
            "LEECH_CAPTION_STYLE": ("_lcaption_style", ""),
        }

        for key, (attr, default) in settings_map.items():
            setattr(
                self,
                attr,
                self._listener.user_dict.get(key) or getattr(Config, key, default),
            )

        if isinstance(self._lcaption, dict):
            self._lcaption = self._lcaption.get("text", "")

        if self._thumb is not None and self._thumb != "none" and not await aiopath.exists(self._thumb):
            self._thumb = None

    async def _msg_to_reply(self):
        """Send initial status message"""
        msg = f"""<blockquote><b><i>VPS Storage Started</i></b></blockquote>

 • <b>User:</b> {self._listener.user.mention} (#ID{self._listener.user_id})
 • <b>Source:</b> <a href='{self._listener.source_url}'>Click Here</a>
 • <b>Destination:</b> <code>{STORAGE_PATH}</code>"""
        
        try:
            self._status_message = await self._listener.message.reply(
                text=msg,
                disable_web_page_preview=True
            )
            self._log_msg = self._status_message
            return True
        except Exception as e:
            await self._cleanup_auto_thumb()
            await self._listener.on_upload_error(str(e))
            return False

    async def _prepare_file(self, pre_file_, dirpath):
        """Prepare filename with prefix/suffix (simplified for local storage)"""
        cap_file_ = file_ = pre_file_

        if len(file_) > 255:
            if is_archive(file_):
                name = get_base_name(file_)
                ext = file_.split(name, 1)[1]
            elif match := re_match(r".+(?=\..+\.0*\d+$)|.+(?=\.part\d+\..+$)", file_):
                name = match.group(0)
                ext = file_.split(name, 1)[1]
            elif len(fsplit := ospath.splitext(file_)) > 1:
                name = fsplit[0]
                ext = fsplit[1]
            else:
                name = file_
                ext = ""
            name = name[: 255 - len(ext)]
            file_ = f"{name}{ext}"

        if pre_file_ != file_:
            new_path = ospath.join(dirpath, file_)
            await rename(self._up_path, new_path)
            self._up_path = new_path

        # Add prefix/suffix if configured
        if self._lprefix:
            file_ = self._lprefix + file_
        if self._lsuffix:
            name, ext = ospath.splitext(file_)
            file_ = name + self._lsuffix + ext

        return cap_mono, file_

    async def _store_file(self, file_, f_path, f_size):
        """Store file to /data/shared with progress simulation"""
        dest_path = ospath.join(STORAGE_PATH, file_)
        
        # Create subdirectories if needed
        dest_dir = ospath.dirname(dest_path)
        await sync_to_async(makedirs, dest_dir, exist_ok=True)
        
        # Simulate upload progress while copying
        chunk_size = 1024 * 1024  # 1MB chunks for progress simulation
        processed = 0
        
        # Copy file with progress simulation
        await sync_to_async(self._copy_with_progress, f_path, dest_path, f_size)
        
        return dest_path

    def _copy_with_progress(self, src, dst, total_size):
        """Synchronous copy with progress updates"""
        import shutil
        
        chunk_size = 1024 * 1024  # 1MB
        copied = 0
        
        with open(src, 'rb') as fsrc:
            with open(dst, 'wb') as fdst:
                while True:
                    chunk = fsrc.read(chunk_size)
                    if not chunk:
                        break
                    fdst.write(chunk)
                    copied += len(chunk)
                    
                    # Simulate progress (will be handled by the async wrapper)
                    if hasattr(self, '_progress_callback'):
                        self._progress_callback(copied, total_size)

    async def upload(self):
        """Main upload method - stores files locally instead of Telegram"""
        await self._user_settings()
        res = await self._msg_to_reply()
        if not res:
            return
        
        # Ensure storage directory exists
        await sync_to_async(makedirs, STORAGE_PATH, exist_ok=True)
        
        is_log_del = False
        for dirpath, _, files in natsorted(await sync_to_async(walk, self._path)):
            if dirpath.strip().endswith("/yt-dlp-thumb"):
                continue
            for file_ in natsorted(files):
                self._error = ""
                self._up_path = f_path = ospath.join(dirpath, file_)
                
                if not await aiopath.exists(self._up_path):
                    LOGGER.error(f"{self._up_path} not exists! Continue uploading!")
                    continue
                
                try:
                    f_size = await aiopath.getsize(self._up_path)

                    is_allowed, reason = await check_strict_file_mode(self._up_path, file_)
                    if not is_allowed:
                        LOGGER.info(f"STRICT_FILE_MODE: Skipping {reason}: {self._up_path}")
                        await remove(self._up_path)
                        continue
                    else:
                        if Config.STRICT_FILE_MODE:
                            LOGGER.info(f"STRICT_FILE_MODE: Storing video {file_} ({f_size / (1024*1024):.2f}MB)")

                    self._total_files += 1

                    if f_size == 0:
                        LOGGER.error(
                            f"{self._up_path} size is zero, skipping"
                        )
                        self._corrupted += 1
                        continue
                    
                    if self._listener.is_cancelled:
                        return
                    
                    await self._user_settings()
                    cap_mono, file_ = await self._prepare_file(file_, dirpath)
                    
                    self._last_uploaded = 0
                    
                    # Store file locally
                    dest_path = await self._store_file(file_, f_path, f_size)
                    
                    # Update total processed bytes
                    self._processed_bytes += f_size
                    
                    LOGGER.info(f"Stored: {file_} -> {dest_path}")
                    
                    # Update status message
                    if self._status_message:
                        try:
                            progress_text = self._get_progress_text()
                            await self._status_message.edit_text(
                                f"{progress_text}\n\n✅ **Last file:** {file_}"
                            )
                        except Exception:
                            pass
                    
                    if self._log_msg and not is_log_del and Config.CLEAN_LOG_MSG:
                        await delete_message(self._log_msg)
                        is_log_del = True
                    
                    if self._listener.is_cancelled:
                        return
                    
                    await sleep(0.5)
                    
                except CancelledUpload:
                    return
                except Exception as err:
                    LOGGER.error(f"{err}. Path: {self._up_path}", exc_info=True)
                    self._error = str(err)
                    self._corrupted += 1
                    if self._listener.is_cancelled:
                        return
                
                if not self._listener.is_cancelled and await aiopath.exists(self._up_path):
                    await remove(self._up_path)
        
        if self._listener.is_cancelled:
            return
        
        if self._total_files == 0:
            await self._cleanup_auto_thumb()
            await self._listener.on_upload_error(
                "No files to store. This may be because Strict Mode is enabled or "
                "because all files match the Excluded Extensions."
            )
            return
        
        if self._total_files <= self._corrupted:
            await self._cleanup_auto_thumb()
            await self._listener.on_upload_error(
                f"Files Corrupted or unable to store. {self._error or 'Check logs!'}"
            )
            return
        
        LOGGER.info(f"Storage Completed: {self._listener.name}")
        
        # Final status message
        elapsed = time() - self._start_time
        if self._status_message:
            final_text = (
                f"✅ **Storage Complete**\n\n"
                f"**Files stored:** {self._total_files}\n"
                f"**Corrupted:** {self._corrupted}\n"
                f"**Total size:** {get_readable_file_size(self._processed_bytes)}\n"
                f"**Elapsed:** {get_readable_time(elapsed)}\n"
                f"**Location:** <code>{STORAGE_PATH}</code>"
            )
            await self._status_message.edit_text(final_text)
        
        await self._cleanup_auto_thumb()
        await self._listener.on_upload_complete(
            None, self._msgs_dict, self._total_files, self._corrupted
        )
        return

    async def _cleanup_auto_thumb(self):
        if self._auto_thumb_path:
            from bot.helper.thumbnail_utils import ThumbnailFetcher
            await ThumbnailFetcher.cleanup_thumbnail(self._auto_thumb_path)
            self._auto_thumb_path = None

    @property
    def speed(self):
        try:
            return self._processed_bytes / (time() - self._start_time)
        except ZeroDivisionError:
            return 0

    @property
    def processed_bytes(self):
        return self._processed_bytes

    async def cancel_task(self):
        self._listener.is_cancelled = True
        await self._cleanup_auto_thumb()
        await self._listener.on_upload_error("your storage operation has been stopped!")
