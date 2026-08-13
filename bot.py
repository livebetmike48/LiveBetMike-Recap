"""
LiveBetMike Plays Recap bot -- AUTONOMOUS edition.

Unlike the sheet-relay recap (which only posts when the sheet is edited),
this bot reads the picks channel ITSELF, parses each posted play, grades
it against real MLB box scores (StatsAPI, free), and posts a nightly
recap at 12:30 AM ET with the day's record, units, YTD and ROI.

Play format it parses (Mike's canonical skeleton; everything after the
market -- book lines, screenshots -- is ignored):

    Risk 1.02 To Win 1U: Tyler Phillips (Marlins) UNDER 14.5 Outs. ...

Brand rule: a play the parser can't read is listed in the recap as
"couldn't grade" -- never guessed at. Units come straight from the play
text (win = +to_win, loss = -risk, push = 0), so no odds math can drift.

Multi-sport by design: the sport of a play = which channel it was posted
in (/setpickschannel per sport). v1 grades MLB; other sports' plays are
stored and listed pending until their graders are wired.

Setup: enable MESSAGE CONTENT INTENT in the Discord dev portal (Bot tab)
-- reading message text is a privileged intent. Railway vars: DISCORD_TOKEN,
DB_PATH (volume path, e.g. /data/recap.db).
"""
import os
import re
import logging
import asyncio
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone, time as dtime

import requests
import discord
from discord import app_commands
from discord.ext import tasks
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")
DB_PATH = os.getenv("DB_PATH", "recap_bot.db")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("recap_bot")

MLB_BASE = "https://statsapi.mlb.com/api/v1"
SPORTS = ["MLB", "WNBA", "NBA", "NFL"]
GRADED_SPORTS = {"MLB", "WNBA"}  # graders wired so far

# ---------------------------------------------------------------- storage
@contextmanager
def _conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    with _conn() as c:
        c.execute("CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT)")
        c.execute("""
            CREATE TABLE IF NOT EXISTS plays (
                message_id TEXT PRIMARY KEY,
                sport TEXT, post_date TEXT,
                raw TEXT, player TEXT, team TEXT,
                side TEXT, point REAL, market TEXT,
                risk REAL, to_win REAL,
                status TEXT DEFAULT 'pending',   -- pending/win/loss/push/ungraded
                actual REAL, profit REAL DEFAULT 0
            )
        """)


def set_config(key, value):
    with _conn() as c:
        c.execute("INSERT INTO config (key,value) VALUES (?,?) "
                  "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, str(value)))


def get_config(key):
    with _conn() as c:
        row = c.execute("SELECT value FROM config WHERE key=?", (key,)).fetchone()
        return row["value"] if row else None


# ---------------------------------------------------------------- parsing
# Risk 1.02 To Win 1U: Tyler Phillips (Marlins) UNDER 14.5 Outs. <noise>
PLAY_RE = re.compile(
    r"risk\s+(?P<risk>\d+(?:\.\d+)?)\s*u?\s+to\s+win\s+(?P<to_win>\d+(?:\.\d+)?)\s*u?\s*:?\s*"
    r"(?P<player>[^()]+?)\s*\((?P<team>[^)]+)\)\s+"
    r"(?P<side>over|under|o|u)\s*(?P<point>\d+(?:\.\d+)?|\.\d+)\s+"
    r"(?P<market>[A-Za-z][A-Za-z+/' ]*?)(?=[.\n]|$)",
    re.IGNORECASE,
)

MARKET_MAP = {
    "outs": ("pitching", "outs"),
    "strikeouts": ("pitching", "strikeOuts"), "ks": ("pitching", "strikeOuts"),
    "k's": ("pitching", "strikeOuts"), "k": ("pitching", "strikeOuts"),
    "hits allowed": ("pitching", "hits"),
    "walks": ("pitching", "baseOnBalls"), "walks allowed": ("pitching", "baseOnBalls"),
    "earned runs": ("pitching", "earnedRuns"), "er": ("pitching", "earnedRuns"),
    "hits": ("batting", "hits"),
    "total bases": ("batting", "totalBases"), "tb": ("batting", "totalBases"),
    "h+r+rbi": ("batting", "hrr"), "hits+runs+rbi": ("batting", "hrr"),
}


def parse_play(text: str):
    """Returns a play dict or None if the message doesn't match the skeleton."""
    m = PLAY_RE.search(text or "")
    if not m:
        return None
    market_raw = re.sub(r"\s+", " ", m.group("market").strip().lower())
    side = m.group("side").lower()
    return {
        "risk": float(m.group("risk")),
        "to_win": float(m.group("to_win")),
        "player": m.group("player").strip(),
        "team": m.group("team").strip(),
        "side": "Over" if side in ("over", "o") else "Under",
        "point": float(m.group("point")),
        "market": market_raw,
    }


# ---------------------------------------------------------------- MLB grading
def _norm(name: str) -> str:
    return re.sub(r"[^a-z]", "", (name or "").lower())


def _et_date_str(offset_days: int = 0) -> str:
    et = datetime.now(timezone.utc) - timedelta(hours=4) + timedelta(days=offset_days)
    return et.strftime("%Y-%m-%d")


def _final_game_pks(date_str: str) -> list[int]:
    r = requests.get(f"{MLB_BASE}/schedule", params={"sportId": 1, "date": date_str}, timeout=15)
    r.raise_for_status()
    pks = []
    for d in r.json().get("dates", []):
        for g in d.get("games", []):
            if g.get("status", {}).get("abstractGameState") == "Final":
                pks.append(g["gamePk"])
    return pks


def _find_stat_in_boxscore(box: dict, player_name: str, group: str, field: str):
    want = _norm(player_name)
    for team_side in ("home", "away"):
        players = (box.get("teams", {}).get(team_side, {}) or {}).get("players", {})
        for p in players.values():
            full = (p.get("person") or {}).get("fullName", "")
            if _norm(full) != want and want not in _norm(full):
                continue
            stats = (p.get("stats") or {}).get(group) or {}
            if not stats:
                continue
            if field == "hrr":
                return float(stats.get("hits", 0) + stats.get("runs", 0) + stats.get("rbi", 0))
            if field == "outs":
                ip = str(stats.get("inningsPitched", "0.0"))
                whole, _, frac = ip.partition(".")
                return float(int(whole) * 3 + int(frac or 0))
            if field in stats:
                return float(stats[field])
    return None


def grade_mlb_play(play: dict, date_str: str, boxscore_cache: dict):
    """Returns (status, actual) or ('pending', None) if not gradeable yet."""
    mk = MARKET_MAP.get(play["market"])
    if mk is None:
        return "ungraded", None
    group, field = mk
    try:
        if "pks" not in boxscore_cache:
            boxscore_cache["pks"] = _final_game_pks(date_str)
        for pk in boxscore_cache["pks"]:
            if pk not in boxscore_cache:
                r = requests.get(f"{MLB_BASE}/game/{pk}/boxscore", timeout=20)
                r.raise_for_status()
                boxscore_cache[pk] = r.json()
            actual = _find_stat_in_boxscore(boxscore_cache[pk], play["player"], group, field)
            if actual is not None:
                if actual == play["point"]:
                    return "push", actual
                went_over = actual > play["point"]
                won = went_over if play["side"] == "Over" else not went_over
                return ("win" if won else "loss"), actual
    except Exception as e:
        log.error("Grading fetch failed for %s: %s", play["player"], e)
        return "pending", None
    return "pending", None  # player not found in any final box yet


# ---------------------------------------------------------------- WNBA grading
# Source: ESPN public JSON (no key). Scoreboard lists the day's games;
# summary?event= carries per-player box scores with a labels/stats layout.
ESPN_WNBA = "https://site.api.espn.com/apis/site/v2/sports/basketball/wnba"

WNBA_MARKET_MAP = {
    "points": "PTS", "pts": "PTS",
    "rebounds": "REB", "rebs": "REB", "reb": "REB",
    "assists": "AST", "asts": "AST", "ast": "AST",
    "threes": "3PM", "3s": "3PM", "3pt": "3PM", "3-pointers": "3PM",
    "three pointers": "3PM", "3 pointers": "3PM", "threes made": "3PM",
    "p+r+a": ("PTS", "REB", "AST"), "pra": ("PTS", "REB", "AST"),
    "p+r": ("PTS", "REB"), "p+a": ("PTS", "AST"), "r+a": ("REB", "AST"),
}


def _wnba_completed_events(date_str: str) -> list[str]:
    r = requests.get(f"{ESPN_WNBA}/scoreboard",
                     params={"dates": date_str.replace("-", "")}, timeout=15)
    r.raise_for_status()
    out = []
    for ev in r.json().get("events", []):
        if ((ev.get("status") or {}).get("type") or {}).get("completed"):
            out.append(str(ev["id"]))
    return out


def _wnba_player_stats(summary: dict, player_name: str):
    """Returns {label: value} for the player from an ESPN summary, or None."""
    want = _norm(player_name)
    for team in (summary.get("boxscore") or {}).get("players", []):
        for block in team.get("statistics", []):
            labels = block.get("labels") or block.get("names") or []
            for ath in block.get("athletes", []):
                full = ((ath.get("athlete") or {}).get("displayName", ""))
                if _norm(full) != want and want not in _norm(full):
                    continue
                vals = ath.get("stats") or []
                if not vals:
                    continue
                return dict(zip(labels, vals))
    return None


def _wnba_stat_value(stats: dict, key: str):
    if key == "3PM":
        made = str(stats.get("3PT", "0-0")).split("-")[0]
        return float(made or 0)
    v = stats.get(key)
    if v is None:
        return None
    try:
        return float(v)
    except ValueError:
        return None


def grade_wnba_play(play: dict, date_str: str, cache: dict):
    mk = WNBA_MARKET_MAP.get(play["market"])
    if mk is None:
        return "ungraded", None
    try:
        if "events" not in cache:
            cache["events"] = _wnba_completed_events(date_str)
        for ev_id in cache["events"]:
            if ev_id not in cache:
                r = requests.get(f"{ESPN_WNBA}/summary", params={"event": ev_id}, timeout=20)
                r.raise_for_status()
                cache[ev_id] = r.json()
            stats = _wnba_player_stats(cache[ev_id], play["player"])
            if stats is None:
                continue
            if isinstance(mk, tuple):
                parts = [_wnba_stat_value(stats, k) for k in mk]
                if any(p is None for p in parts):
                    return "pending", None
                actual = float(sum(parts))
            else:
                actual = _wnba_stat_value(stats, mk)
                if actual is None:
                    return "pending", None
            if actual == play["point"]:
                return "push", actual
            went_over = actual > play["point"]
            won = went_over if play["side"] == "Over" else not went_over
            return ("win" if won else "loss"), actual
    except Exception as e:
        log.error("WNBA grading fetch failed for %s: %s", play["player"], e)
        return "pending", None
    return "pending", None


GRADERS = {"MLB": grade_mlb_play, "WNBA": grade_wnba_play}


# ---------------------------------------------------------------- bot
intents = discord.Intents.default()
intents.message_content = True  # requires the portal toggle


class RecapBot(discord.Client):
    def __init__(self):
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)

    async def setup_hook(self):
        init_db()
        sport_choices = [app_commands.Choice(name=s, value=s) for s in SPORTS]

        @app_commands.choices(sport=sport_choices)
        async def _setpicks(interaction: discord.Interaction,
                            sport: app_commands.Choice[str]):
            if not interaction.user.guild_permissions.administrator:
                await interaction.response.send_message("Admin only.", ephemeral=True)
                return
            set_config(f"picks_channel_{sport.value}", interaction.channel_id)
            await interaction.response.send_message(
                f"✅ Scanning {interaction.channel.mention} for **{sport.value}** plays."
                + ("" if sport.value in GRADED_SPORTS else
                   f" (Note: {sport.value} grading isn't wired yet — plays will be stored and listed pending.)"))

        self.tree.add_command(app_commands.Command(
            name="setpickschannel",
            description="ADMIN: scan this channel's posts as plays for a sport",
            callback=_setpicks))

        @app_commands.choices(sport=sport_choices)
        async def _setrecap(interaction: discord.Interaction,
                            sport: app_commands.Choice[str]):
            if not interaction.user.guild_permissions.administrator:
                await interaction.response.send_message("Admin only.", ephemeral=True)
                return
            set_config(f"recap_channel_{sport.value}", interaction.channel_id)
            await interaction.response.send_message(
                f"✅ Nightly **{sport.value}** recap will post in {interaction.channel.mention}.")

        self.tree.add_command(app_commands.Command(
            name="setrecapchannel",
            description="ADMIN: post a sport's nightly recap in this channel",
            callback=_setrecap))

        @app_commands.choices(sport=sport_choices)
        async def _recapnow(interaction: discord.Interaction,
                            sport: app_commands.Choice[str]):
            if not interaction.user.guild_permissions.administrator:
                await interaction.response.send_message("Admin only.", ephemeral=True)
                return
            await interaction.response.defer(ephemeral=True)
            status = await run_recap(self, sport.value, _et_date_str(0))
            await interaction.followup.send(f"**{sport.value}** recap: {status}", ephemeral=True)

        self.tree.add_command(app_commands.Command(
            name="recapnow",
            description="ADMIN: scan, grade and post today's recap for a sport right now",
            callback=_recapnow))

        @app_commands.choices(sport=sport_choices)
        async def _record(interaction: discord.Interaction,
                          sport: app_commands.Choice[str]):
            with _conn() as c:
                rows = c.execute(
                    "SELECT status, COUNT(*) n, SUM(profit) p, SUM(risk) r FROM plays "
                    "WHERE sport=? AND status IN ('win','loss','push') GROUP BY status",
                    (sport.value,)).fetchall()
            wins = sum(r["n"] for r in rows if r["status"] == "win")
            losses = sum(r["n"] for r in rows if r["status"] == "loss")
            pushes = sum(r["n"] for r in rows if r["status"] == "push")
            profit = sum(r["p"] or 0 for r in rows)
            risked = sum(r["r"] or 0 for r in rows)
            roi = f"{profit / risked * 100:+.2f}%" if risked else "—"
            await interaction.response.send_message(
                f"**{sport.value} YTD**: {wins}-{losses}" + (f"-{pushes}" if pushes else "")
                + f" | {profit:+.2f}u | ROI {roi}")

        self.tree.add_command(app_commands.Command(
            name="record",
            description="Show a sport's season record, units and ROI",
            callback=_record))

        try:
            synced = await self.tree.sync()
            log.info("Synced %d slash commands", len(synced))
        except Exception as e:
            log.error("Slash command sync failed: %s", e)

    async def on_ready(self):
        log.info("Logged in as %s", self.user)
        if not nightly_recap.is_running():
            nightly_recap.start(self)


async def scan_channel_for_plays(bot: RecapBot, sport: str, date_str: str) -> int:
    """Reads the sport's picks channel for messages posted on date_str (ET)
    and stores any new parseable plays. Returns count of new plays stored."""
    channel_id = get_config(f"picks_channel_{sport}")
    if not channel_id:
        return 0
    channel = bot.get_channel(int(channel_id))
    if channel is None:
        return 0
    day_start_utc = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(hours=4)
    day_end_utc = day_start_utc + timedelta(hours=24)
    new = 0
    async for msg in channel.history(after=day_start_utc, before=day_end_utc, limit=500):
        if msg.author.bot:
            continue
        play = parse_play(msg.content)
        with _conn() as c:
            if play is None:
                if (msg.content or "").strip():
                    c.execute("INSERT OR IGNORE INTO plays (message_id, sport, post_date, raw, status) "
                              "VALUES (?,?,?,?, 'ungraded')",
                              (str(msg.id), sport, date_str, msg.content[:300]))
                continue
            cur = c.execute(
                "INSERT OR IGNORE INTO plays (message_id, sport, post_date, raw, player, team, "
                "side, point, market, risk, to_win) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (str(msg.id), sport, date_str, msg.content[:300], play["player"], play["team"],
                 play["side"], play["point"], play["market"], play["risk"], play["to_win"]))
            new += cur.rowcount
    return new


async def run_recap(bot: RecapBot, sport: str, date_str: str) -> str:
    await scan_channel_for_plays(bot, sport, date_str)
    with _conn() as c:
        rows = [dict(r) for r in c.execute(
            "SELECT * FROM plays WHERE sport=? AND post_date=?", (sport, date_str)).fetchall()]
    if not rows:
        return "no plays found for " + date_str

    if sport in GRADED_SPORTS:
        grader = GRADERS[sport]
        cache: dict = {}
        for r in rows:
            if r["status"] != "pending" or not r["player"]:
                continue
            status, actual = await asyncio.to_thread(grader, r, date_str, cache)
            profit = r["to_win"] if status == "win" else (-r["risk"] if status == "loss" else 0)
            with _conn() as c:
                c.execute("UPDATE plays SET status=?, actual=?, profit=? WHERE message_id=?",
                          (status, actual, profit, r["message_id"]))
            r.update(status=status, actual=actual, profit=profit)

    graded = [r for r in rows if r["status"] in ("win", "loss", "push")]
    pending = [r for r in rows if r["status"] == "pending"]
    ungraded = [r for r in rows if r["status"] == "ungraded"]
    wins = sum(1 for r in graded if r["status"] == "win")
    losses = sum(1 for r in graded if r["status"] == "loss")
    day_profit = sum(r["profit"] or 0 for r in graded)

    with _conn() as c:
        tot = c.execute("SELECT SUM(profit) p, SUM(risk) r FROM plays WHERE sport=? "
                        "AND status IN ('win','loss','push')", (sport,)).fetchone()
    ytd = tot["p"] or 0
    roi = f"{(tot['p'] or 0) / tot['r'] * 100:+.2f}%" if tot["r"] else "—"

    mm, dd, yy = date_str[5:7].lstrip("0"), date_str[8:10].lstrip("0"), date_str[2:4]
    embed = discord.Embed(
        title=f"{sport} Recap — {mm}/{dd}/{yy} | {day_profit:+.2f}u",
        description=f"**{wins}-{losses} today** ({len(graded)} bet{'s' if len(graded) != 1 else ''})",
        color=discord.Color.green() if day_profit >= 0 else discord.Color.red(),
    )
    lines = []
    for r in graded:
        mark = {"win": "✅", "loss": "❌", "push": "➖"}[r["status"]]
        actual = f" (actual: {r['actual']:g})" if r["actual"] is not None else ""
        lines.append(f"{mark} {r['player']} {r['side']} {r['point']:g} {r['market']}{actual} — {r['profit']:+.2f}u")
    if lines:
        embed.add_field(name="Plays", value="\n".join(lines)[:1024], inline=False)
    if pending:
        embed.add_field(name="⏳ Still pending (game not final / player not found)",
                        value="\n".join(f"{r['player']} {r['side']} {r['point']:g} {r['market']}"
                                        for r in pending)[:1024], inline=False)
    if ungraded:
        embed.add_field(name="🔎 Couldn't grade — check manually",
                        value="\n".join((r["raw"] or "?")[:80] for r in ungraded)[:1024], inline=False)
    embed.add_field(name="💰 Units YTD", value=f"{ytd:+.2f}u", inline=True)
    embed.add_field(name="📈 ROI", value=roi, inline=True)

    channel_id = get_config(f"recap_channel_{sport}") or get_config(f"picks_channel_{sport}")
    channel = bot.get_channel(int(channel_id)) if channel_id else None
    if channel is None:
        return "recap channel not set"
    await channel.send(embed=embed)
    return f"posted — {wins}-{losses}, {day_profit:+.2f}u, {len(pending)} pending, {len(ungraded)} unreadable"


# 04:30 UTC = 12:30 AM ET (EDT) -- recaps the ET day that just ended
@tasks.loop(time=dtime(hour=4, minute=30))
async def nightly_recap(bot: RecapBot):
    date_str = _et_date_str(-1) if datetime.now(timezone.utc).hour < 12 else _et_date_str(0)
    for sport in SPORTS:
        if get_config(f"picks_channel_{sport}"):
            try:
                status = await run_recap(bot, sport, date_str)
                log.info("%s nightly recap: %s", sport, status)
            except Exception as e:
                log.error("%s nightly recap failed: %s", sport, e)


@nightly_recap.before_loop
async def _before():
    await client.wait_until_ready()


client = RecapBot()

if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit("Set DISCORD_TOKEN")
    client.run(TOKEN)
