import asyncio, os, sys, re, sqlite3
import html
from telethon.sessions import StringSession
from pathlib import Path
from getpass import getpass
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import ChannelPrivateError, SessionPasswordNeededError
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ==== настройки размера/объёмов ====
MAX_PER_CHAT = 6      # максимум вакансий из одного канала за отправку
MAX_TOTAL    = 60      # общий максимум вакансий в одном обходе
CHUNK_LEN    = 2200    # символов на одно сообщение (безопасно < 4096)
SNIPPET_LEN  = 140     # длина сниппета в строке дайджеста

# ==== базовая инициализация ====
BASE = Path(__file__).parent
load_dotenv(BASE / ".env")

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
PHONE = os.getenv("PHONE", "")
TZ = os.getenv("TIMEZONE", "Asia/Nicosia")
SEND_TO_ME = os.getenv("SEND_TO_ME", "true").lower() == "true"
TARGET_CHAT = os.getenv("TARGET_CHAT", "").strip()

DB = BASE / "state.sqlite"
SRC_FILE = BASE / "sources.txt"
TOPICS_FILE = BASE / "topics_config.txt"  # Новый файл конфигурации
SESSION = BASE / "tg_user.session"


def require(cond: bool, msg: str):
    if not cond:
        print(msg)
        sys.exit(1)


def load_sources():
    require(SRC_FILE.exists(), "Нет файла sources.txt — создайте его в папке проекта.")
    with open(SRC_FILE, "r", encoding="utf-8") as f:
        return [
            line.strip()
            .replace("https://t.me/", "")
            .replace("http://t.me/", "")
            .replace("t.me/", "")
            for line in f
            if line.strip() and not line.strip().startswith("#")
        ]


def load_topics_config():
    """
    Читает topics_config.txt и создает:
    - keywords_dict: словарь {ключевое_слово: id_темы}
    - exclude_list: список исключающих слов
    """
    if not TOPICS_FILE.exists():
        return None, None

    keywords_dict = {}
    exclude_list = []
    current_section = None
    current_topic_id = 1  # По умолчанию в общий чат

    for line in TOPICS_FILE.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        
        # Пропускаем пустые строки и комментарии
        if not line or line.startswith("#"):
            continue
        
        # Определяем секцию
        if line == "[include]":
            current_section = "include"
            continue
        elif line == "[exclude]":
            current_section = "exclude"
            continue
        
        # Обрабатываем содержимое секций
        if current_section == "include":
            # Проверяем, является ли строка определением темы <НАЗВАНИЕ:ID>
            topic_match = re.match(r'<([^:]+):(\d+)>', line)
            if topic_match:
                topic_name, topic_id = topic_match.groups()
                current_topic_id = int(topic_id)
                print(f"Загружена тема: {topic_name} -> ID: {current_topic_id}")
            else:
                # Обычное ключевое слово - добавляем с текущим ID темы
                keywords_dict[line.lower()] = current_topic_id
                
        elif current_section == "exclude":
            exclude_list.append(line.lower())
    
    print(f"Загружено ключевых слов: {len(keywords_dict)}, исключений: {len(exclude_list)}")
    return keywords_dict, exclude_list


def get_topic_for_text(text, keywords_dict):
    """Определяет ID темы для текста на основе ключевых слов"""
    if not keywords_dict:
        return 1  # По умолчанию в общий чат
    
    text_lower = text.lower()
    
    # Ищем первое подходящее ключевое слово
    for keyword, topic_id in keywords_dict.items():
        if keyword in text_lower:
            return topic_id
    
    return 1  # Если не нашли - в общий чат


def highlight_kw(src_text: str, keywords_dict):
    """
    Возвращает HTML-безопасный текст с <b>подсветкой</b> ключевых слов.
    """
    if not src_text or not keywords_dict:
        return html.escape(src_text)

    text_lower = src_text.lower()
    found_keywords = []
    
    # Находим все ключевые слова в тексте
    for keyword in keywords_dict.keys():
        if keyword in text_lower:
            # Находим все вхождения этого ключевого слова
            start = 0
            while True:
                pos = text_lower.find(keyword, start)
                if pos == -1:
                    break
                found_keywords.append((pos, pos + len(keyword), keyword))
                start = pos + 1
    
    if not found_keywords:
        return html.escape(src_text)
    
    # Сортируем по позиции и подсвечиваем
    found_keywords.sort()
    out = []
    last = 0
    
    for start, end, keyword in found_keywords:
        # Часть до совпадения
        out.append(html.escape(src_text[last:start]))
        # Совпадение с подсветкой
        out.append("<b>" + html.escape(src_text[start:end]) + "</b>")
        last = end
    
    # Хвост
    out.append(html.escape(src_text[last:]))
    return "".join(out)


def db():
    """
    Открывает (или создаёт) SQLite-базу данных и гарантирует,
    что таблица offsets существует.
    Таблица хранит последнее просмотренное сообщение для каждого чата.
    """
    con = sqlite3.connect(DB)
    con.execute("""
        CREATE TABLE IF NOT EXISTS offsets (
            chat TEXT PRIMARY KEY,
            last_id INTEGER NOT NULL DEFAULT 0
        )
    """)
    con.commit()  # фиксируем создание таблицы
    return con


async def ensure_login(client: TelegramClient):
    if not await client.is_user_authorized():
        # иногда Телеграм может вернуть AuthRestartError — повторяем ещё раз
        for _ in range(2):
            try:
                await client.send_code_request(PHONE)
                break
            except Exception as e:
                print(f"Повторная отправка кода: {e}")
        code = input("Введите код из Telegram: ").strip()
        try:
            await client.sign_in(PHONE, code)
        except SessionPasswordNeededError:
            pwd = getpass("Включена 2FA. Введите пароль от Telegram: ")
            await client.sign_in(password=pwd)
    me = await client.get_me()
    print(f"Вошли как: {getattr(me,'first_name', '')} (@{getattr(me,'username', '')})")


async def resolve_target_dialog(client: TelegramClient):
    """Определяем, куда слать дайджест."""
    if SEND_TO_ME or not TARGET_CHAT:
        return "me"
    async for d in client.iter_dialogs():
        if (d.name or "").strip().lower() == TARGET_CHAT.lower():
            return d.entity
    print(
        f"[WARN] Чат '{TARGET_CHAT}' не найден среди ваших диалогов. "
        f"Отправлю в Избранное."
    )
    return "me"


def split_into_chunks(text: str, limit: int):
    """
    Режем текст гарантированно: по строкам, а если строка всё равно длиннее лимита — рубим её на куски.
    Не даём ни одному куску превысить limit.
    """
    chunks, buf, ln = [], [], 0
    lines = text.split("\n")
    for line in lines:
        # если сама строка длиннее лимита — порежем её жёстко
        while len(line) > limit:
            # добиваем текущий буфер, если он уже непустой
            if buf:
                chunks.append("\n".join(buf))
                buf, ln = [], 0
            chunks.append(line[:limit])
            line = line[limit:]
        # теперь строка гарантированно <= limit
        add = len(line) + 1  # + перенос
        if ln + add > limit and buf:
            chunks.append("\n".join(buf))
            buf, ln = [line], add
        else:
            buf.append(line)
            ln += add
    if buf:
        chunks.append("\n".join(buf))
    return chunks


async def scan_once(client: TelegramClient, target):
    sources = load_sources()
    keywords_dict, exclude_list = load_topics_config()
    con = db()

    hits = []
    scanned = 0        # сколько источников обработали
    seen_msgs = 0      # сколько сообщений просмотрели (после last_id)

    if not sources:
        msg = "Новых совпадений нет. Источников: 0."
        await client.send_message(target, msg)
        return msg

    for src in sources:
        scanned += 1
        try:
            entity = await client.get_entity(src)
        except Exception as e:
            print(f"[WARN] Не удалось открыть {src}: {e}")
            continue

        row = con.execute("SELECT last_id FROM offsets WHERE chat=?", (src,)).fetchone()
        last_id = row[0] if row else 0
        max_id = last_id

        try:
            async for m in client.iter_messages(entity, limit=300, min_id=last_id):
                seen_msgs += 1
                text = (m.message or "")
                if not text:
                    continue
                    
                text_lower = text.lower()
                
                # Проверяем исключения
                if exclude_list and any(exclude_word in text_lower for exclude_word in exclude_list):
                    continue
                    
                # Проверяем ключевые слова
                if keywords_dict and not any(keyword in text_lower for keyword in keywords_dict.keys()):
                    continue
                
                # Определяем тему для вакансии
                topic_id = get_topic_for_text(text, keywords_dict)
                link = f"https://t.me/{src}/{m.id}"
                
                # Безопасная подсветка ключевых слов
                safe = highlight_kw(text, keywords_dict)
                snippet = re.sub(r"\s+", " ", safe)[:SNIPPET_LEN]
                
                hits.append((src, m.id, snippet, link, topic_id))
                if m.id > max_id:
                    max_id = m.id
                    
        except ChannelPrivateError:
            print(f"[WARN] Нет доступа к {src}. Убедитесь, что вы подписаны.")
            continue
        except Exception as e:
            print(f"[WARN] Ошибка при чтении {src}: {e}")
            continue

        if max_id > last_id:
            con.execute(
                "INSERT INTO offsets(chat,last_id) VALUES(?,?) "
                "ON CONFLICT(chat) DO UPDATE SET last_id=excluded.last_id",
                (src, max_id),
            )
            con.commit()

    # ---- НЕТ СОВПАДЕНИЙ → ВСЕГДА отправляем уведомление
    if not hits:
        msg = f"Новых совпадений нет. Проверено источников: {scanned}, просмотрено сообщений: {seen_msgs}."
        await client.send_message(target, msg)
        return msg

    # Группируем по темам
    by_topic = {}
    for src, mid, snip, link, topic_id in hits:
        by_topic.setdefault(topic_id, []).append((src, mid, snip, link))

    # Собираем и отправляем дайджесты по темам
    total_sent = 0
    for topic_id, topic_hits in by_topic.items():
        # Сортируем по свежести
        topic_hits = sorted(topic_hits, key=lambda x: x[1], reverse=True)[:MAX_TOTAL]
        
        # Группируем по каналам
        by_chat = {}
        for src, mid, snip, link in topic_hits:
            by_chat.setdefault(src, []).append((mid, snip, link))

        # Собираем дайджест для темы
        lines = []
        for src, items in by_chat.items():
            items = sorted(items, key=lambda x: x[0], reverse=True)[:MAX_PER_CHAT]
            lines.append(f"🔎 <b>{html.escape(src)}</b> — {len(items)} шт.")
            for _, snip, link in items:
                lines.append(f"• <a href=\"{link}\">открыть</a> — {snip}")
            lines.append("")

        if lines:
            html_content = "\n".join(lines)
            chunks = split_into_chunks(html_content, CHUNK_LEN)
            
            for i, part in enumerate(chunks, 1):
                head = f"<b>Дайджест ({i}/{len(chunks)})</b>\n" if len(chunks) > 1 else ""
                try:
                    await client.send_message(
                        entity=target,
                        message=head + part,
                        reply_to=topic_id if topic_id != 1 else None,
                        parse_mode="html",
                        link_preview=False
                    )
                    total_sent += 1
                except Exception as e:
                    print(f"Ошибка отправки в тему {topic_id}: {e}")

    return f"Отправлено сообщений: {total_sent}, совпадений: {len(hits)}."


async def main():
    require(API_ID and API_HASH and PHONE, "Заполните .env (API_ID, API_HASH, PHONE)")
    STRING_SESSION = os.getenv("STRING_SESSION", "").strip()
    client = TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH) if STRING_SESSION else TelegramClient(str(SESSION), API_ID, API_HASH)
    await client.connect()
    await ensure_login(client)
    target = await resolve_target_dialog(client)

    # разовый прогон при старте
    print(await scan_once(client, target))

    # расписание 3 раза в день
    scheduler = AsyncIOScheduler(timezone=TZ)
    hours_str = os.getenv("RUN_HOURS", "10,14,18")
    try:
        hours = [int(h.strip()) for h in hours_str.split(",") if h.strip().isdigit()]
    except Exception as e:
        print(f"[WARN] Ошибка чтения RUN_HOURS={hours_str!r}: {e}")
        hours = [10, 14, 18]

    for h in hours:
        scheduler.add_job(scan_once, "cron", hour=h, minute=0, args=[client, target])
        print(f"Добавлена задача: {h:02d}:00")

    scheduler.start()
    print(f"Планировщик активен: {', '.join(f'{h:02d}:00' for h in hours)}")
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        pass
    finally:
        await client.disconnect()
 

if __name__ == "__main__":
    asyncio.run(main())