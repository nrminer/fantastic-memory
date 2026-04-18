import os, json, random, string, socket, hashlib
from flask import Flask, request, jsonify, render_template, g
import sqlite3

app = Flask(__name__)
DATABASE = 'casino.db'

# ─── DB helpers ──────────────────────────────────────────────────────────────

def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA journal_mode=WAL')
    return db

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

SCHEMA = '''
CREATE TABLE IF NOT EXISTS players (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    name          TEXT NOT NULL,
    email         TEXT DEFAULT '',
    phone         TEXT DEFAULT '',
    vip_level     TEXT DEFAULT 'Standard',
    notes         TEXT DEFAULT '',
    password_hash TEXT DEFAULT '',
    created_at    TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS transactions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id   INTEGER NOT NULL,
    amount      REAL NOT NULL,
    game_type   TEXT DEFAULT 'Muu',
    note        TEXT DEFAULT '',
    created_at  TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (player_id) REFERENCES players(id)
);
CREATE TABLE IF NOT EXISTS bonuses (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id   INTEGER NOT NULL,
    label       TEXT NOT NULL DEFAULT 'Bonus',
    amount      REAL DEFAULT 0,
    claimed     INTEGER DEFAULT 0,
    seen        INTEGER DEFAULT 1,
    created_at  TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (player_id) REFERENCES players(id)
);
CREATE TABLE IF NOT EXISTS poker_sessions (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    status               TEXT DEFAULT 'waiting',
    deck_json            TEXT DEFAULT '[]',
    community_cards_json TEXT DEFAULT '[]',
    stage                TEXT DEFAULT 'waiting',
    preset_hands_json    TEXT DEFAULT '{}',
    created_at           TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS poker_seats (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id          INTEGER NOT NULL,
    player_name         TEXT NOT NULL,
    player_id           INTEGER DEFAULT NULL,
    hole_cards_json     TEXT DEFAULT '[]',
    folded              INTEGER DEFAULT 0,
    active              INTEGER DEFAULT 1,
    show_cards          INTEGER DEFAULT 0,
    join_token          TEXT NOT NULL UNIQUE,
    seat_number         INTEGER NOT NULL,
    FOREIGN KEY (session_id) REFERENCES poker_sessions(id)
);
'''

def _hash_pw(pw):
    return hashlib.sha256(pw.encode()).hexdigest()

def init_db():
    db = sqlite3.connect(DATABASE)
    db.executescript(SCHEMA)
    migrations = [
        'ALTER TABLE poker_seats    ADD COLUMN show_cards        INTEGER DEFAULT 0',
        'ALTER TABLE poker_seats    ADD COLUMN player_id         INTEGER DEFAULT NULL',
        'ALTER TABLE poker_sessions ADD COLUMN preset_hands_json TEXT DEFAULT "{}"',
        "CREATE TABLE IF NOT EXISTS bonuses (id INTEGER PRIMARY KEY AUTOINCREMENT, player_id INTEGER NOT NULL, label TEXT NOT NULL DEFAULT 'Bonus', amount REAL DEFAULT 0, claimed INTEGER DEFAULT 0, seen INTEGER DEFAULT 1, created_at TEXT DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY (player_id) REFERENCES players(id))",
        "ALTER TABLE players ADD COLUMN password_hash TEXT DEFAULT ''",
        "ALTER TABLE bonuses ADD COLUMN seen INTEGER DEFAULT 1",
        "ALTER TABLE players ADD COLUMN spins_remaining INTEGER DEFAULT 0",
        "ALTER TABLE players ADD COLUMN points INTEGER DEFAULT 0",
        """CREATE TABLE IF NOT EXISTS point_transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id INTEGER NOT NULL,
            delta INTEGER NOT NULL,
            reason TEXT DEFAULT '',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (player_id) REFERENCES players(id)
        )""",
        """CREATE TABLE IF NOT EXISTS blackjack_games (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id INTEGER NOT NULL,
            bet INTEGER NOT NULL,
            deck_json TEXT NOT NULL,
            player_cards_json TEXT NOT NULL,
            dealer_cards_json TEXT NOT NULL,
            status TEXT DEFAULT 'active',
            result_json TEXT DEFAULT '',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (player_id) REFERENCES players(id)
        )""",
        "ALTER TABLE players ADD COLUMN streak_mode TEXT DEFAULT 'normal'",
        """CREATE TABLE IF NOT EXISTS system_settings (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )""",
        """CREATE TABLE IF NOT EXISTS pikapokeri_games (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id   INTEGER NOT NULL,
            bet         INTEGER NOT NULL,
            deck_json   TEXT NOT NULL,
            hand_json   TEXT NOT NULL,
            status      TEXT DEFAULT 'deal',
            payout      INTEGER DEFAULT 0,
            result_rank INTEGER DEFAULT -1,
            result_name TEXT DEFAULT '',
            created_at  TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (player_id) REFERENCES players(id)
        )""",
        "ALTER TABLE blackjack_games ADD COLUMN insurance_bet INTEGER DEFAULT 0",
    ]
    for m in migrations:
        try:
            db.execute(m)
        except Exception:
            pass
    db.commit()
    db.close()

init_db()

# ─── Deck utils ──────────────────────────────────────────────────────────────

SUITS = ['♠', '♥', '♦', '♣']
RANKS = ['2','3','4','5','6','7','8','9','10','J','Q','K','A']

def new_deck():
    deck = [{'rank': r, 'suit': s} for s in SUITS for r in RANKS]
    random.shuffle(deck)
    return deck

def gen_token():
    return ''.join(random.choices(string.ascii_letters + string.digits, k=24))

def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return '127.0.0.1'

def current_session(db):
    row = db.execute('SELECT * FROM poker_sessions ORDER BY id DESC LIMIT 1').fetchone()
    return dict(row) if row else None

# ─── Poker hand evaluation ────────────────────────────────────────────────────

HAND_NAMES = {
    9: 'Royal Flush', 8: 'Värisuora',   7: 'Nelikko',
    6: 'Full House',  5: 'Väri',        4: 'Suora',
    3: 'Kolmikko',    2: 'Kaksi paria', 1: 'Pari', 0: 'Korkein kortti',
}
_RV = {'2':2,'3':3,'4':4,'5':5,'6':6,'7':7,'8':8,'9':9,'10':10,'J':11,'Q':12,'K':13,'A':14}

def _eval5(cards):
    from collections import Counter
    vals  = sorted([_RV[c['rank']] for c in cards], reverse=True)
    suits = [c['suit'] for c in cards]
    flush = len(set(suits)) == 1
    uv    = sorted(set(vals), reverse=True)
    straight, hi = False, 0
    if len(uv) == 5 and uv[0] - uv[4] == 4:
        straight, hi = True, uv[0]
    elif uv == [14,5,4,3,2]:
        straight, hi, vals = True, 5, [5,4,3,2,1]
    cnt    = Counter(vals)
    groups = sorted(cnt.items(), key=lambda x: (x[1], x[0]), reverse=True)
    cl     = [g[1] for g in groups]
    vl     = [g[0] for g in groups]
    if straight and flush: return (9 if hi == 14 else 8, [hi])
    if cl[0] == 4:         return (7, vl)
    if cl[0] == 3 and cl[1] == 2: return (6, vl)
    if flush:              return (5, vals)
    if straight:           return (4, [hi])
    if cl[0] == 3:         return (3, vl)
    if cl[0] == 2 and cl[1] == 2: return (2, vl)
    if cl[0] == 2:         return (1, vl)
    return (0, vals)

def best_hand(hole, community):
    from itertools import combinations
    best = None
    for combo in combinations(hole + community, 5):
        r = _eval5(list(combo))
        if best is None or r > best:
            best = r
    return best

# ─── Spin prizes ─────────────────────────────────────────────────────────────

SPIN_PRIZES = [
    {'bonus': 10,  'label': '10% Matchausbonus',  'weight': 80},
    {'bonus': 20,  'label': '20% Matchausbonus',  'weight': 12},
    {'bonus': 50,  'label': '50% Matchausbonus',  'weight': 5},
    {'bonus': 100, 'label': '100% Matchausbonus', 'weight': 3},
]

# ─── Pages ───────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return render_template('index.html', local_ip=get_local_ip())

@app.route('/poker/join')
def poker_join_page():
    return render_template('poker_player.html')

@app.route('/asiakas')
def customer_page():
    return render_template('customer.html', local_ip=get_local_ip())

@app.route('/manifest.json')
def pwa_manifest():
    from flask import Response
    manifest = {
        "name": "Kasino",
        "short_name": "Kasino",
        "start_url": "/asiakas",
        "display": "standalone",
        "background_color": "#0a1a10",
        "theme_color": "#0a1a10",
        "orientation": "portrait",
        "icons": [
            {"src": "data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><rect width='100' height='100' fill='%230a1a10'/><text y='.9em' font-size='80' x='10'>♠</text></svg>",
             "sizes": "any", "type": "image/svg+xml"}
        ]
    }
    return Response(json.dumps(manifest), mimetype='application/manifest+json')

# ─── Players API ─────────────────────────────────────────────────────────────

@app.route('/api/players', methods=['GET'])
def list_players():
    db = get_db()
    q   = request.args.get('q',   '').strip().lower()
    vip = request.args.get('vip', '').strip()
    rows = db.execute('''
        SELECT p.*,
            COALESCE(SUM(CASE WHEN t.amount >  0 THEN  t.amount ELSE 0 END), 0) AS total_won,
            COALESCE(SUM(CASE WHEN t.amount <  0 THEN -t.amount ELSE 0 END), 0) AS total_lost,
            COALESCE(SUM(t.amount), 0) AS net_balance,
            COUNT(t.id) AS tx_count
        FROM players p
        LEFT JOIN transactions t ON t.player_id = p.id
        GROUP BY p.id ORDER BY p.name
    ''').fetchall()
    players = [dict(r) for r in rows]
    # Strip password hash from list response, but expose has_password flag
    for p in players:
        has_pw = bool(p.get('password_hash', ''))
        p.pop('password_hash', None)
        p['has_password']    = has_pw
        p['spins_remaining'] = p.get('spins_remaining') or 0
        p['points']          = p.get('points') or 0
        p['streak_mode']     = p.get('streak_mode') or 'normal'
    if q:
        players = [p for p in players if q in p['name'].lower()
                   or q in (p['email'] or '').lower()
                   or q in (p['phone'] or '').lower()]
    if vip:
        players = [p for p in players if p['vip_level'] == vip]
    return jsonify(players)

@app.route('/api/players', methods=['POST'])
def create_player():
    d  = request.json
    db = get_db()
    pw = (d.get('password') or '').strip()
    pw_hash = _hash_pw(pw) if pw else ''
    cur = db.execute(
        'INSERT INTO players(name,email,phone,vip_level,notes,password_hash) VALUES(?,?,?,?,?,?)',
        (d['name'], d.get('email',''), d.get('phone',''),
         d.get('vip_level','Standard'), d.get('notes',''), pw_hash)
    )
    db.commit()
    row = dict(db.execute('SELECT * FROM players WHERE id=?', (cur.lastrowid,)).fetchone())
    row.pop('password_hash', None)
    row['has_password'] = bool(pw_hash)
    row.update({'total_won': 0, 'total_lost': 0, 'net_balance': 0, 'tx_count': 0})
    return jsonify(row), 201

@app.route('/api/players/<int:pid>', methods=['PUT'])
def update_player(pid):
    d  = request.json
    db = get_db()
    pw = (d.get('password') or '').strip()
    if pw:
        pw_hash = _hash_pw(pw)
        db.execute(
            'UPDATE players SET name=?,email=?,phone=?,vip_level=?,notes=?,password_hash=? WHERE id=?',
            (d['name'], d.get('email',''), d.get('phone',''),
             d.get('vip_level','Standard'), d.get('notes',''), pw_hash, pid)
        )
    else:
        db.execute(
            'UPDATE players SET name=?,email=?,phone=?,vip_level=?,notes=? WHERE id=?',
            (d['name'], d.get('email',''), d.get('phone',''),
             d.get('vip_level','Standard'), d.get('notes',''), pid)
        )
    db.commit()
    return jsonify({'ok': True})

@app.route('/api/players/<int:pid>', methods=['DELETE'])
def delete_player(pid):
    db = get_db()
    db.execute('DELETE FROM transactions WHERE player_id=?', (pid,))
    db.execute('DELETE FROM bonuses WHERE player_id=?', (pid,))
    db.execute('DELETE FROM players WHERE id=?', (pid,))
    db.commit()
    return jsonify({'ok': True})

@app.route('/api/players/<int:pid>/grant-spins', methods=['POST'])
def grant_spins(pid):
    d  = request.json or {}
    try:
        count = int(d.get('count', 0))
    except (TypeError, ValueError):
        return jsonify({'error': 'Virheellinen määrä.'}), 400
    if count == 0:
        return jsonify({'error': 'Määrä ei voi olla 0.'}), 400
    db = get_db()
    row = db.execute('SELECT * FROM players WHERE id=?', (pid,)).fetchone()
    if not row:
        return jsonify({'error': 'Pelaajaa ei löydy.'}), 404
    current = (row['spins_remaining'] or 0) if 'spins_remaining' in row.keys() else 0
    new_val = max(0, current + count)
    db.execute('UPDATE players SET spins_remaining=? WHERE id=?', (new_val, pid))
    db.commit()
    return jsonify({'ok': True, 'spins_remaining': new_val, 'granted': count})

@app.route('/api/players/<int:pid>/spins', methods=['GET'])
def get_spins(pid):
    db = get_db()
    row = db.execute('SELECT spins_remaining FROM players WHERE id=?', (pid,)).fetchone()
    if not row:
        return jsonify({'error': 'Pelaajaa ei löydy.'}), 404
    return jsonify({'spins_remaining': row['spins_remaining'] or 0})

@app.route('/api/players/<int:pid>/transactions', methods=['GET'])
def player_transactions(pid):
    rows = get_db().execute(
        'SELECT * FROM transactions WHERE player_id=? ORDER BY created_at DESC', (pid,)
    ).fetchall()
    return jsonify([dict(r) for r in rows])

@app.route('/api/players/<int:pid>/transactions', methods=['POST'])
def add_transaction(pid):
    d   = request.json
    db  = get_db()
    cur = db.execute(
        'INSERT INTO transactions(player_id,amount,game_type,note) VALUES(?,?,?,?)',
        (pid, float(d['amount']), d.get('game_type','Muu'), d.get('note',''))
    )
    db.commit()
    return jsonify(dict(db.execute('SELECT * FROM transactions WHERE id=?', (cur.lastrowid,)).fetchone())), 201

@app.route('/api/transactions/<int:tid>', methods=['DELETE'])
def delete_transaction(tid):
    db = get_db()
    db.execute('DELETE FROM transactions WHERE id=?', (tid,))
    db.commit()
    return jsonify({'ok': True})

# ─── Bonuses API ─────────────────────────────────────────────────────────────

@app.route('/api/players/<int:pid>/bonuses', methods=['GET'])
def get_player_bonuses(pid):
    rows = get_db().execute(
        'SELECT * FROM bonuses WHERE player_id=? ORDER BY created_at DESC', (pid,)
    ).fetchall()
    return jsonify([dict(r) for r in rows])

@app.route('/api/players/<int:pid>/bonuses', methods=['POST'])
def add_bonus(pid):
    d   = request.json
    db  = get_db()
    # seen=0 → triggers real-time notification on player device; seen=1 → silent
    # notify:true (default) means seen=0 so the player gets a pop-up
    seen_val = 0 if d.get('notify', True) else 1
    cur = db.execute(
        'INSERT INTO bonuses(player_id,label,amount,seen) VALUES(?,?,?,?)',
        (pid, d.get('label','Bonus'), float(d.get('amount', 0)), seen_val)
    )
    db.commit()
    return jsonify(dict(db.execute('SELECT * FROM bonuses WHERE id=?', (cur.lastrowid,)).fetchone())), 201

@app.route('/api/bonuses/<int:bid>', methods=['DELETE'])
def delete_bonus(bid):
    db = get_db()
    db.execute('DELETE FROM bonuses WHERE id=?', (bid,))
    db.commit()
    return jsonify({'ok': True})

@app.route('/api/bonuses/<int:bid>/seen', methods=['POST'])
def mark_bonus_seen(bid):
    db = get_db()
    db.execute('UPDATE bonuses SET seen=1 WHERE id=?', (bid,))
    db.commit()
    return jsonify({'ok': True})

@app.route('/api/bonuses/<int:bid>/claim', methods=['POST'])
def claim_bonus(bid):
    db = get_db()
    bonus = db.execute('SELECT * FROM bonuses WHERE id=?', (bid,)).fetchone()
    if not bonus:
        return jsonify({'error': 'Bonusta ei löydy.'}), 404
    if bonus['claimed']:
        return jsonify({'error': 'Bonus on jo lunastettu.', 'already_claimed': True}), 400
    db.execute('UPDATE bonuses SET claimed=1, seen=1 WHERE id=?', (bid,))
    db.commit()
    claimed = dict(db.execute('SELECT * FROM bonuses WHERE id=?', (bid,)).fetchone())
    return jsonify({'ok': True, 'bonus': claimed, 'amount': claimed['amount'], 'label': claimed['label']})

# ─── Dashboard API ───────────────────────────────────────────────────────────

@app.route('/api/dashboard')
def dashboard():
    db = get_db()
    house_rev = db.execute('SELECT COALESCE(SUM(-amount),0) FROM transactions WHERE amount<0').fetchone()[0]
    paid_out  = db.execute('SELECT COALESCE(SUM(amount),0)  FROM transactions WHERE amount>0').fetchone()[0]
    n_players = db.execute('SELECT COUNT(*) FROM players').fetchone()[0]
    n_tx      = db.execute('SELECT COUNT(*) FROM transactions').fetchone()[0]

    top_losers = [dict(r) for r in db.execute('''
        SELECT p.id,p.name,p.vip_level,COALESCE(SUM(t.amount),0) AS net
        FROM players p LEFT JOIN transactions t ON t.player_id=p.id
        GROUP BY p.id HAVING net<0 ORDER BY net ASC LIMIT 5
    ''').fetchall()]

    top_winners = [dict(r) for r in db.execute('''
        SELECT p.id,p.name,p.vip_level,COALESCE(SUM(t.amount),0) AS net
        FROM players p LEFT JOIN transactions t ON t.player_id=p.id
        GROUP BY p.id HAVING net>0 ORDER BY net DESC LIMIT 5
    ''').fetchall()]

    recent = [dict(r) for r in db.execute('''
        SELECT t.*, p.name AS player_name, p.vip_level
        FROM transactions t JOIN players p ON p.id=t.player_id
        ORDER BY t.created_at DESC LIMIT 12
    ''').fetchall()]

    by_game = [dict(r) for r in db.execute('''
        SELECT game_type, COUNT(*) AS cnt,
               COALESCE(SUM(CASE WHEN amount<0 THEN -amount ELSE 0 END),0) AS house_take
        FROM transactions GROUP BY game_type ORDER BY house_take DESC
    ''').fetchall()]

    return jsonify({
        'house_revenue': house_rev, 'total_payouts': paid_out,
        'net_house': house_rev - paid_out, 'total_players': n_players,
        'total_transactions': n_tx, 'top_losers': top_losers,
        'top_winners': top_winners, 'recent_transactions': recent, 'by_game': by_game,
    })

# ─── Customer API ─────────────────────────────────────────────────────────────

@app.route('/api/customer/login', methods=['POST'])
def customer_login():
    data     = request.json or {}
    name     = (data.get('name') or '').strip()
    password = (data.get('password') or '').strip()
    if not name:
        return jsonify({'error': 'Syötä käyttäjänimi.'}), 400
    db = get_db()
    player = db.execute('SELECT * FROM players WHERE LOWER(name)=LOWER(?)', (name,)).fetchone()
    if not player:
        return jsonify({'error': 'Käyttäjää ei löydy. Pyydä kassohenkilökuntaa rekisteröimään sinut.'}), 404
    p = dict(player)
    # Password check: if player has password set, verify it
    if p.get('password_hash'):
        if not password:
            return jsonify({'error': 'Tili vaatii salasanan.', 'needs_password': True}), 401
        if _hash_pw(password) != p['password_hash']:
            return jsonify({'error': 'Väärä salasana.'}), 401
    # Return player data (no password hash)
    p.pop('password_hash', None)
    p['has_password'] = bool(player['password_hash'])
    p['spins_remaining'] = p.get('spins_remaining') or 0
    p['points']          = p.get('points') or 0
    bonuses = [dict(r) for r in db.execute(
        'SELECT * FROM bonuses WHERE player_id=? ORDER BY created_at DESC', (p['id'],)
    ).fetchall()]
    p['bonuses'] = bonuses
    return jsonify(p)

# ─── Poker API ───────────────────────────────────────────────────────────────

@app.route('/api/poker/state')
def poker_state():
    db   = get_db()
    sess = current_session(db)
    if not sess:
        return jsonify({'status': 'none'})
    sess['community_cards']  = json.loads(sess['community_cards_json'])
    sess['preset_hands']     = json.loads(sess.get('preset_hands_json') or '{}')
    seats = [dict(r) for r in db.execute(
        'SELECT * FROM poker_seats WHERE session_id=? ORDER BY seat_number', (sess['id'],)
    ).fetchall()]
    for s in seats:
        s['hole_cards'] = json.loads(s['hole_cards_json'])
    sess['seats'] = seats
    return jsonify(sess)

@app.route('/api/poker/new', methods=['POST'])
def poker_new():
    db  = get_db()
    cur = db.execute(
        'INSERT INTO poker_sessions(status,deck_json,community_cards_json,stage,preset_hands_json) VALUES(?,?,?,?,?)',
        ('waiting', json.dumps(new_deck()), '[]', 'waiting', '{}')
    )
    db.commit()
    return jsonify({'id': cur.lastrowid, 'status': 'waiting'})

@app.route('/api/poker/join', methods=['POST'])
def poker_join_api():
    d    = request.json
    db   = get_db()
    sess = current_session(db)
    name = (d.get('name') or '').strip()
    if not name:
        return jsonify({'error': 'Nimi vaaditaan.'}), 400
    if not sess:
        return jsonify({'error': 'Ei avoimia pelejä — pyydä jakajaa aloittamaan peli.'}), 400
    # Always allow rejoining an existing active seat — even if the game is already running.
    # This handles re-login on a new device or after clearing the browser.
    existing = db.execute(
        'SELECT * FROM poker_seats WHERE session_id=? AND player_name=? AND active=1',
        (sess['id'], name)
    ).fetchone()
    if existing:
        return jsonify({'token': existing['join_token'], 'seat': existing['seat_number'], 'name': name})
    # New seats can only be added when the game is in waiting state
    if sess['status'] != 'waiting':
        return jsonify({'error': 'Peli on jo käynnissä — odotetaan seuraavaa kierrosta.'}), 400
    count = db.execute(
        'SELECT COUNT(*) FROM poker_seats WHERE session_id=? AND active=1', (sess['id'],)
    ).fetchone()[0]
    if count >= 9:
        return jsonify({'error': 'Pöytä täynnä (max 9 pelaajaa).'}), 400
    token     = gen_token()
    player_id = d.get('player_id')
    db.execute(
        'INSERT INTO poker_seats(session_id,player_name,player_id,join_token,seat_number) VALUES(?,?,?,?,?)',
        (sess['id'], name, player_id, token, count + 1)
    )
    db.commit()
    return jsonify({'token': token, 'seat': count + 1, 'name': name})

@app.route('/api/poker/deal', methods=['POST'])
def poker_deal():
    db    = get_db()
    sess  = current_session(db)
    if not sess:
        return jsonify({'error': 'Ei istuntoa.'}), 400
    deck    = json.loads(sess['deck_json'])
    presets = json.loads(sess.get('preset_hands_json') or '{}')
    seats   = db.execute(
        'SELECT * FROM poker_seats WHERE session_id=? AND active=1 ORDER BY seat_number', (sess['id'],)
    ).fetchall()
    if not seats:
        return jsonify({'error': 'Ei pelaajia pöydässä.'}), 400

    used = {(c['rank'], c['suit']) for cards in presets.values() for c in cards if isinstance(cards, list)}
    deck = [c for c in deck if (c['rank'], c['suit']) not in used]
    if len(deck) < len(seats) * 2 + 5:
        full = new_deck()
        deck = [c for c in full if (c['rank'], c['suit']) not in used]

    for seat in seats:
        sid = str(seat['id'])
        if sid in presets and len(presets[sid]) == 2:
            cards = presets[sid]
        else:
            cards = [deck.pop(), deck.pop()]
        db.execute('UPDATE poker_seats SET hole_cards_json=?,folded=0 WHERE id=?',
                   (json.dumps(cards), seat['id']))

    # Preserve community presets across deal; clear only player hole-card presets
    comm_preset = presets.get('community', [])
    new_presets = {'community': comm_preset} if comm_preset else {}
    db.execute(
        'UPDATE poker_sessions SET deck_json=?,stage=?,status=?,community_cards_json=?,preset_hands_json=? WHERE id=?',
        (json.dumps(deck), 'preflop', 'active', '[]', json.dumps(new_presets), sess['id'])
    )
    db.commit()
    return jsonify({'ok': True, 'stage': 'preflop'})

@app.route('/api/poker/preset', methods=['POST'])
def poker_preset():
    d    = request.json or {}
    db   = get_db()
    sess = current_session(db)
    if not sess:
        return jsonify({'error': 'Ei istuntoa.'}), 400
    db.execute('UPDATE poker_sessions SET preset_hands_json=? WHERE id=?',
               (json.dumps(d), sess['id']))
    db.commit()
    return jsonify({'ok': True})

@app.route('/api/poker/advance', methods=['POST'])
def poker_advance():
    db   = get_db()
    sess = current_session(db)
    if not sess:
        return jsonify({'error': 'Ei istuntoa.'}), 400
    deck      = json.loads(sess['deck_json'])
    community = json.loads(sess['community_cards_json'])
    stage     = sess['stage']
    comm_pre  = json.loads(sess.get('preset_hands_json') or '{}').get('community', [])
    def _cc(idx):
        """Return preset community card at index, or pop from deck."""
        if idx < len(comm_pre) and comm_pre[idx]:
            return comm_pre[idx]
        return deck.pop()
    if stage == 'preflop':
        community = [_cc(0), _cc(1), _cc(2)]; new_stage = 'flop'
    elif stage == 'flop':
        community.append(_cc(3)); new_stage = 'turn'
    elif stage == 'turn':
        community.append(_cc(4)); new_stage = 'river'
    elif stage == 'river':
        new_stage = 'showdown'
    else:
        return jsonify({'error': 'Ei voida edetä tästä vaiheesta.'}), 400
    db.execute(
        'UPDATE poker_sessions SET deck_json=?,stage=?,community_cards_json=? WHERE id=?',
        (json.dumps(deck), new_stage, json.dumps(community), sess['id'])
    )
    db.commit()
    return jsonify({'ok': True, 'stage': new_stage, 'community_cards': community})

@app.route('/api/poker/void', methods=['POST'])
def poker_void():
    db   = get_db()
    sess = current_session(db)
    if not sess:
        return jsonify({'error': 'Ei istuntoa.'}), 400
    db.execute(
        'UPDATE poker_sessions SET deck_json=?,stage=?,community_cards_json=?,status=?,preset_hands_json=? WHERE id=?',
        (json.dumps(new_deck()), 'waiting', '[]', 'waiting', '{}', sess['id'])
    )
    db.execute('UPDATE poker_seats SET hole_cards_json=?,folded=0,show_cards=0 WHERE session_id=? AND active=1',
               ('[]', sess['id']))
    db.commit()
    return jsonify({'ok': True})

@app.route('/api/poker/fold/<int:seat_id>', methods=['POST'])
def poker_fold(seat_id):
    db = get_db()
    db.execute('UPDATE poker_seats SET folded=1 WHERE id=?', (seat_id,))
    db.commit()
    return jsonify({'ok': True})

@app.route('/api/poker/remove/<int:seat_id>', methods=['DELETE'])
def poker_remove(seat_id):
    db = get_db()
    db.execute('UPDATE poker_seats SET active=0 WHERE id=?', (seat_id,))
    db.commit()
    return jsonify({'ok': True})

@app.route('/api/poker/player/<token>/showcards', methods=['POST'])
def toggle_show_cards(token):
    db   = get_db()
    seat = db.execute('SELECT * FROM poker_seats WHERE join_token=?', (token,)).fetchone()
    if not seat:
        return jsonify({'error': 'Virheellinen tunnus.'}), 404
    new_val = 0 if seat['show_cards'] else 1
    db.execute('UPDATE poker_seats SET show_cards=? WHERE join_token=?', (new_val, token))
    db.commit()
    return jsonify({'show_cards': bool(new_val)})

@app.route('/api/poker/player/<token>')
def poker_player_state(token):
    db   = get_db()
    seat = db.execute('SELECT * FROM poker_seats WHERE join_token=?', (token,)).fetchone()
    if not seat:
        return jsonify({'error': 'Virheellinen tunnus.'}), 404
    seat = dict(seat)
    sess = dict(db.execute('SELECT * FROM poker_sessions WHERE id=?', (seat['session_id'],)).fetchone())
    n_active = db.execute(
        'SELECT COUNT(*) FROM poker_seats WHERE session_id=? AND active=1', (sess['id'],)
    ).fetchone()[0]
    return jsonify({
        'name':            seat['player_name'],
        'seat':            seat['seat_number'],
        'hole_cards':      json.loads(seat['hole_cards_json']),
        'folded':          bool(seat['folded']),
        'active':          bool(seat['active']),
        'show_cards':      bool(seat['show_cards']),
        'stage':           sess['stage'],
        'community_cards': json.loads(sess['community_cards_json']),
        'status':          sess['status'],
        'n_players':       n_active,
    })

@app.route('/api/poker/evaluate')
def poker_evaluate():
    db        = get_db()
    sess      = current_session(db)
    if not sess:
        return jsonify({'error': 'Ei istuntoa.'}), 400
    community = json.loads(sess['community_cards_json'])
    if len(community) < 3:
        return jsonify({'error': 'Tarvitaan vähintään flop arviointia varten.'}), 400
    seats = [dict(r) for r in db.execute(
        'SELECT * FROM poker_seats WHERE session_id=? AND active=1 AND folded=0 ORDER BY seat_number',
        (sess['id'],)
    ).fetchall()]
    results = []
    for s in seats:
        hole = json.loads(s['hole_cards_json'])
        if len(hole) != 2:
            continue
        rank, tb = best_hand(hole, community)
        results.append({
            'seat_id':     s['id'],
            'seat_number': s['seat_number'],
            'player_name': s['player_name'],
            'hole_cards':  hole,
            'hand_rank':   rank,
            'hand_name':   HAND_NAMES[rank],
            'tiebreakers': tb,
        })
    results.sort(key=lambda r: (r['hand_rank'], r['tiebreakers']), reverse=True)
    if results:
        top = results[0]
        for r in results:
            r['is_winner'] = (r['hand_rank'] == top['hand_rank']
                              and r['tiebreakers'] == top['tiebreakers'])
    return jsonify(results)

@app.route('/api/poker/spin', methods=['POST'])
def poker_spin():
    d         = request.json or {}
    player_id = d.get('player_id')
    if not player_id:
        return jsonify({'error': 'Kirjaudu sisään pyöräyttääksesi.'}), 401
    db = get_db()
    row = db.execute('SELECT * FROM players WHERE id=?', (player_id,)).fetchone()
    if not row:
        return jsonify({'error': 'Pelaajaa ei löydy.'}), 404
    remaining = row['spins_remaining'] or 0
    if remaining <= 0:
        return jsonify({'error': 'Sinulla ei ole pyöräytyksiä. Pyydä kassohenkilökunnalta.',
                        'spins_remaining': 0}), 403
    # Atomic decrement — only succeeds if remaining > 0
    cur = db.execute(
        'UPDATE players SET spins_remaining = spins_remaining - 1 '
        'WHERE id=? AND spins_remaining > 0', (player_id,)
    )
    db.commit()
    if cur.rowcount == 0:
        return jsonify({'error': 'Ei pyöräytyksiä jäljellä.', 'spins_remaining': 0}), 403
    new_remaining = (db.execute('SELECT spins_remaining FROM players WHERE id=?',
                                (player_id,)).fetchone()['spins_remaining']) or 0

    total = sum(p['weight'] for p in SPIN_PRIZES)
    r     = random.uniform(0, total)
    cum   = 0
    idx, prize = 0, SPIN_PRIZES[0]
    for i, pr in enumerate(SPIN_PRIZES):
        cum += pr['weight']
        if r <= cum:
            idx, prize = i, pr
            break

    # Auto-create the bonus so the player can redeem the prize from the bonuses panel.
    label = f"Pyöräytys: {prize['label']}"
    db.execute(
        'INSERT INTO bonuses(player_id,label,amount,seen) VALUES(?,?,?,?)',
        (player_id, label, float(prize['bonus']), 0)
    )
    db.commit()

    return jsonify({'prize': prize, 'index': idx, 'spins_remaining': new_remaining})

# ─── Points system ───────────────────────────────────────────────────────────
# Prize catalog — redemption costs are in points.
PRIZES_CATALOG = [
    {'id': 'cash5',   'cost':  500, 'label': '€5 kassabonus',                'kind': 'cash', 'amount':  5},
    {'id': 'cash10', 'cost': 1000, 'label': '€10 kassabonus',               'kind': 'cash', 'amount': 10},
    {'id': 'cash25', 'cost': 2250, 'label': '€25 kassabonus',               'kind': 'cash', 'amount': 25},
    {'id': 'cash50', 'cost': 4000, 'label': '€50 kassabonus',               'kind': 'cash', 'amount': 50},
    {'id': 'spin1',  'cost':  300, 'label': '1 onnenpyörän pyöräytys',      'kind': 'spin', 'spins': 1},
    {'id': 'spin5',  'cost': 1200, 'label': '5 onnenpyörän pyöräytystä',    'kind': 'spin', 'spins': 5},
]
PRIZE_BY_ID = {p['id']: p for p in PRIZES_CATALOG}

# Mini-game constraints
MIN_BET, MAX_BET = 10, 10000

# Pikapokeri (Jacks-or-Better video poker) payout multipliers
PIKAPOKERI_PAYOUTS = {9: 800, 8: 50, 7: 25, 6: 9, 5: 6, 4: 4, 3: 3, 2: 2, 1: 1}
PIKAPOKERI_NAMES   = {
    9: 'Royal Flush', 8: 'Värisuora', 7: 'Nelikko',
    6: 'Full House',  5: 'Väri',      4: 'Suora',
    3: 'Kolmikko',    2: 'Kaksi paria', 1: 'Pari (J tai parempi)', -1: 'Häviö',
}

# Default system settings
SETTINGS_DEFAULTS = {
    'points_per_eur':   '10',   # 100 pts = €10
    'min_redeem_pts':   '500',
    'max_redeem_pts':   '5000',
    'point_expiry_days':'365',
}

def _get_streak_mode(db, pid):
    row = db.execute('SELECT streak_mode FROM players WHERE id=?', (pid,)).fetchone()
    if not row: return 'normal'
    return row['streak_mode'] or 'normal'

def _get_setting(db, key):
    row = db.execute('SELECT value FROM system_settings WHERE key=?', (key,)).fetchone()
    return row['value'] if row else SETTINGS_DEFAULTS.get(key, '')

def _pikapokeri_eval(cards):
    """Returns (rank, multiplier, name). rank=-1 = losing hand."""
    rank, tiebreakers = _eval5(cards)
    if rank == 0:
        return -1, 0, 'Häviö'
    if rank == 1:
        if tiebreakers[0] < 11:
            return -1, 0, 'Häviö'
        return 1, 1, 'Pari (J tai parempi)'
    mult = PIKAPOKERI_PAYOUTS.get(rank, 0)
    return rank, mult, PIKAPOKERI_NAMES.get(rank, 'Häviö')

def _log_points(db, pid, delta, reason):
    db.execute('INSERT INTO point_transactions(player_id,delta,reason) VALUES(?,?,?)',
               (pid, int(delta), reason))

def _atomic_deduct_points(db, pid, amount, reason):
    """Atomically deduct points. Returns new balance on success, None if insufficient."""
    row = db.execute('SELECT points FROM players WHERE id=?', (pid,)).fetchone()
    if not row:
        return None
    if (row['points'] or 0) < amount:
        return None
    cur = db.execute(
        'UPDATE players SET points = points - ? WHERE id=? AND points >= ?',
        (amount, pid, amount)
    )
    if cur.rowcount == 0:
        return None
    _log_points(db, pid, -amount, reason)
    new_bal = db.execute('SELECT points FROM players WHERE id=?', (pid,)).fetchone()['points'] or 0
    return new_bal

def _add_points(db, pid, amount, reason):
    db.execute('UPDATE players SET points = points + ? WHERE id=?', (amount, pid))
    _log_points(db, pid, amount, reason)
    return db.execute('SELECT points FROM players WHERE id=?', (pid,)).fetchone()['points'] or 0

def _get_bet(req, player_id, db):
    """Extract + validate bet, return (bet, error_response_or_None)."""
    try:
        bet = int(req.get('bet', 0))
    except (TypeError, ValueError):
        return 0, (jsonify({'error': 'Virheellinen panos.'}), 400)
    if bet < MIN_BET or bet > MAX_BET:
        return 0, (jsonify({'error': f'Panoksen oltava {MIN_BET}–{MAX_BET} pistettä.'}), 400)
    row = db.execute('SELECT points FROM players WHERE id=?', (player_id,)).fetchone()
    if not row:
        return 0, (jsonify({'error': 'Pelaajaa ei löydy.'}), 404)
    if (row['points'] or 0) < bet:
        return 0, (jsonify({'error': 'Ei tarpeeksi pisteitä.'}), 400)
    return bet, None

# ─── Point admin endpoints ───────────────────────────────────────────────────

@app.route('/api/players/<int:pid>/points', methods=['GET'])
def get_points(pid):
    db  = get_db()
    row = db.execute('SELECT points FROM players WHERE id=?', (pid,)).fetchone()
    if not row:
        return jsonify({'error': 'Pelaajaa ei löydy.'}), 404
    history = [dict(r) for r in db.execute(
        'SELECT * FROM point_transactions WHERE player_id=? ORDER BY created_at DESC LIMIT 30', (pid,)
    ).fetchall()]
    return jsonify({'points': row['points'] or 0, 'history': history})

@app.route('/api/players/<int:pid>/points/grant', methods=['POST'])
def grant_points(pid):
    d = request.json or {}
    try:
        count = int(d.get('count', 0))
    except (TypeError, ValueError):
        return jsonify({'error': 'Virheellinen määrä.'}), 400
    if count == 0:
        return jsonify({'error': 'Määrä ei voi olla 0.'}), 400
    reason = (d.get('reason') or ('Kassan myöntö' if count > 0 else 'Kassan vähennys')).strip()[:120]
    db = get_db()
    row = db.execute('SELECT * FROM players WHERE id=?', (pid,)).fetchone()
    if not row:
        return jsonify({'error': 'Pelaajaa ei löydy.'}), 404
    current = row['points'] or 0
    new_val = max(0, current + count)
    actual_delta = new_val - current
    db.execute('UPDATE players SET points=? WHERE id=?', (new_val, pid))
    _log_points(db, pid, actual_delta, reason)
    db.commit()
    return jsonify({'ok': True, 'points': new_val, 'granted': actual_delta})

@app.route('/api/points/prizes')
def list_prizes():
    return jsonify(PRIZES_CATALOG)

@app.route('/api/players/<int:pid>/points/redeem', methods=['POST'])
def redeem_prize(pid):
    d  = request.json or {}
    pid_prize = d.get('prize_id')
    prize = PRIZE_BY_ID.get(pid_prize)
    if not prize:
        return jsonify({'error': 'Palkintoa ei löydy.'}), 404
    db = get_db()
    new_bal = _atomic_deduct_points(db, pid, prize['cost'], f"Lunastus: {prize['label']}")
    if new_bal is None:
        return jsonify({'error': 'Ei tarpeeksi pisteitä.'}), 400
    if prize['kind'] == 'cash':
        # Create a bonus the player can claim from the bonuses panel
        db.execute(
            'INSERT INTO bonuses(player_id,label,amount,seen) VALUES(?,?,?,?)',
            (pid, f"Pistelunastus: {prize['label']}", float(prize['amount']), 0)
        )
    elif prize['kind'] == 'spin':
        db.execute('UPDATE players SET spins_remaining = spins_remaining + ? WHERE id=?',
                   (int(prize['spins']), pid))
    db.commit()
    return jsonify({'ok': True, 'points': new_bal, 'prize': prize})

# ─── Mini-games ──────────────────────────────────────────────────────────────

def _card_value_bj(rank, soft_ace=True):
    if rank in ('J','Q','K'): return 10
    if rank == 'A':           return 11 if soft_ace else 1
    return int(rank)

def _hand_total(cards):
    total  = sum(_card_value_bj(c['rank']) for c in cards)
    aces   = sum(1 for c in cards if c['rank']=='A')
    while total > 21 and aces > 0:
        total -= 10
        aces  -= 1
    return total

@app.route('/api/points/<int:pid>/coinflip', methods=['POST'])
def game_coinflip(pid):
    d      = request.json or {}
    choice = (d.get('choice') or '').lower()
    if choice not in ('heads','tails'):
        return jsonify({'error': 'Valitse klaava tai kruuna.'}), 400
    db = get_db()
    bet, err = _get_bet(d, pid, db)
    if err: return err
    _atomic_deduct_points(db, pid, bet, f'Kolikonheitto panos ({choice})')
    streak = _get_streak_mode(db, pid)
    if streak == 'win':
        result  = choice
        payout  = bet * 2
        _add_points(db, pid, payout, f'Kolikonheitto voitto ({result})')
        outcome = 'win'
    elif streak == 'lose':
        result  = 'tails' if choice == 'heads' else 'heads'
        outcome, payout = 'loss', 0
    else:
        result = 'heads' if random.random() < 0.50 else 'tails'
        if choice == result and random.random() < 0.96:
            payout  = bet * 2
            _add_points(db, pid, payout, f'Kolikonheitto voitto ({result})')
            outcome = 'win'
        else:
            outcome, payout = 'loss', 0
    db.commit()
    bal = db.execute('SELECT points FROM players WHERE id=?', (pid,)).fetchone()['points'] or 0
    return jsonify({
        'outcome': outcome, 'result': result, 'choice': choice,
        'bet': bet, 'payout': payout, 'net': payout - bet, 'points': bal,
    })

@app.route('/api/points/<int:pid>/war', methods=['POST'])
def game_war(pid):
    d = request.json or {}
    db = get_db()
    bet, err = _get_bet(d, pid, db)
    if err: return err
    _atomic_deduct_points(db, pid, bet, 'Sota-peli panos')
    deck = new_deck()
    pc, dc = deck.pop(), deck.pop()
    pv, dv = _RV[pc['rank']], _RV[dc['rank']]
    streak = _get_streak_mode(db, pid)
    if streak == 'win':
        outcome = 'win';  payout = bet * 2
        _add_points(db, pid, payout, 'Sota-peli voitto')
    elif streak == 'lose':
        outcome = 'loss'; payout = 0
    elif pv > dv:
        outcome = 'win';  payout = bet * 2
        _add_points(db, pid, payout, 'Sota-peli voitto')
    elif pv < dv:
        outcome = 'loss'; payout = 0
    else:
        outcome = 'push'; payout = bet
        _add_points(db, pid, payout, 'Sota-peli tasapeli')
    db.commit()
    bal = db.execute('SELECT points FROM players WHERE id=?', (pid,)).fetchone()['points'] or 0
    return jsonify({
        'outcome': outcome, 'player_card': pc, 'dealer_card': dc,
        'bet': bet, 'payout': payout, 'net': payout - bet, 'points': bal,
    })

@app.route('/api/points/<int:pid>/baccarat', methods=['POST'])
def game_baccarat(pid):
    d    = request.json or {}
    side = (d.get('side') or '').lower()
    if side not in ('player','banker','tie'):
        return jsonify({'error': 'Valitse: player, banker tai tie.'}), 400
    db = get_db()
    bet, err = _get_bet(d, pid, db)
    if err: return err
    _atomic_deduct_points(db, pid, bet, f'Baccarat panos ({side})')
    def val(r):
        if r in ('J','Q','K','10'): return 0
        if r == 'A': return 1
        return int(r)
    deck = new_deck()
    p1, p2, b1, b2 = deck.pop(), deck.pop(), deck.pop(), deck.pop()
    ptot = (val(p1['rank']) + val(p2['rank'])) % 10
    btot = (val(b1['rank']) + val(b2['rank'])) % 10
    phand, bhand = [p1, p2], [b1, b2]
    if ptot < 8 and btot < 8:
        # Player third-card rule
        p3 = None
        if ptot <= 5:
            p3 = deck.pop(); phand.append(p3)
            ptot = (ptot + val(p3['rank'])) % 10
        # Banker third-card rule (simplified)
        draw_banker = False
        if p3 is None:
            draw_banker = btot <= 5
        else:
            p3v = val(p3['rank'])
            if   btot <= 2: draw_banker = True
            elif btot == 3: draw_banker = p3v != 8
            elif btot == 4: draw_banker = p3v in (2,3,4,5,6,7)
            elif btot == 5: draw_banker = p3v in (4,5,6,7)
            elif btot == 6: draw_banker = p3v in (6,7)
        if draw_banker:
            b3 = deck.pop(); bhand.append(b3)
            btot = (btot + val(b3['rank'])) % 10
    if   ptot > btot: winner = 'player'
    elif btot > ptot: winner = 'banker'
    else:             winner = 'tie'
    # Streak override (cards still shown, only outcome changes)
    streak = _get_streak_mode(db, pid)
    if streak == 'win':
        winner = side
    elif streak == 'lose':
        if   side == 'player': winner = 'banker'
        elif side == 'banker': winner = 'player'
        else:                  winner = 'player'  # tie bet → any non-tie
    # Payouts: player 1:1, banker 0.95:1, tie 8:1. Losers on non-tie bet if winner=tie? Classic rule: tie is a push for player/banker bets.
    payout = 0
    if side == winner:
        if   side == 'player': payout = bet * 2
        elif side == 'banker': payout = bet + int(bet * 0.95)
        elif side == 'tie':    payout = bet * 9
        _add_points(db, pid, payout, f'Baccarat voitto ({winner})')
        outcome = 'win'
    elif winner == 'tie' and side in ('player','banker'):
        payout  = bet  # push
        _add_points(db, pid, payout, 'Baccarat tasapeli (palautus)')
        outcome = 'push'
    else:
        outcome = 'loss'
    db.commit()
    bal = db.execute('SELECT points FROM players WHERE id=?', (pid,)).fetchone()['points'] or 0
    return jsonify({
        'outcome': outcome, 'winner': winner, 'side': side,
        'player_hand': phand, 'banker_hand': bhand,
        'player_total': ptot, 'banker_total': btot,
        'bet': bet, 'payout': payout, 'net': payout - bet, 'points': bal,
    })

# ── Blackjack (stateful) ──
def _bj_state(game):
    pcards  = json.loads(game['player_cards_json'])
    dcards  = json.loads(game['dealer_cards_json'])
    ins_bet = game['insurance_bet'] if game['insurance_bet'] is not None else 0
    active  = game['status'] == 'active'
    return {
        'game_id':      game['id'],
        'bet':          game['bet'],
        'status':       game['status'],
        'player_cards': pcards,
        'dealer_cards': dcards if not active else [dcards[0]] + [{'rank':'?','suit':'?'}]*(len(dcards)-1),
        'player_total': _hand_total(pcards),
        'dealer_total': _hand_total(dcards) if not active else _hand_total([dcards[0]]),
        'insurance_available': (
            active and
            len(pcards) == 2 and
            dcards[0]['rank'] == 'A' and
            ins_bet == 0
        ),
        'insurance_bet': ins_bet,
    }

@app.route('/api/points/<int:pid>/blackjack/start', methods=['POST'])
def game_bj_start(pid):
    d = request.json or {}
    db = get_db()
    # Cancel any stale active game
    db.execute("UPDATE blackjack_games SET status='abandoned' WHERE player_id=? AND status='active'", (pid,))
    bet, err = _get_bet(d, pid, db)
    if err: return err
    _atomic_deduct_points(db, pid, bet, 'Blackjack panos')
    deck   = new_deck()
    pc     = [deck.pop(), deck.pop()]
    dc     = [deck.pop(), deck.pop()]
    status = 'active'
    streak = _get_streak_mode(db, pid)
    # Natural blackjack — suppress on lose streak
    if _hand_total(pc) == 21 and streak != 'lose':
        status = 'done_blackjack'
        payout = bet + int(bet * 1.5)  # 3:2
        _add_points(db, pid, payout, 'Blackjack luonnollinen 21')
    cur = db.execute(
        '''INSERT INTO blackjack_games(player_id,bet,deck_json,player_cards_json,dealer_cards_json,status)
           VALUES(?,?,?,?,?,?)''',
        (pid, bet, json.dumps(deck), json.dumps(pc), json.dumps(dc), status)
    )
    gid = cur.lastrowid
    db.commit()
    game = db.execute('SELECT * FROM blackjack_games WHERE id=?', (gid,)).fetchone()
    state = _bj_state(game)
    bal = db.execute('SELECT points FROM players WHERE id=?', (pid,)).fetchone()['points'] or 0
    state['points'] = bal
    if status.startswith('done'):
        state['outcome'] = 'blackjack'
        state['payout']  = bet + int(bet * 1.5)
        state['net']     = int(bet * 1.5)
    return jsonify(state)

@app.route('/api/points/blackjack/<int:gid>/action', methods=['POST'])
def game_bj_action(gid):
    d      = request.json or {}
    action = (d.get('action') or '').lower()
    db     = get_db()
    game   = db.execute('SELECT * FROM blackjack_games WHERE id=?', (gid,)).fetchone()
    if not game:
        return jsonify({'error': 'Peliä ei löydy.'}), 404
    if game['status'] != 'active':
        return jsonify({'error': 'Peli on jo päättynyt.'}), 400
    pid    = game['player_id']
    bet    = game['bet']
    deck   = json.loads(game['deck_json'])
    pcards = json.loads(game['player_cards_json'])
    dcards = json.loads(game['dealer_cards_json'])

    outcome = None; payout = 0

    if action == 'hit':
        pcards.append(deck.pop())
        total = _hand_total(pcards)
        if total > 21:
            status  = 'done_bust'
            outcome = 'bust'
        else:
            status = 'active'
    elif action == 'stand':
        while _hand_total(dcards) < 17:
            dcards.append(deck.pop())
        ptot, dtot = _hand_total(pcards), _hand_total(dcards)
        if dtot > 21 or ptot > dtot:
            status = 'done_win';  outcome = 'win';  payout = bet * 2
        elif ptot == dtot:
            status = 'done_push'; outcome = 'push'; payout = bet
        else:
            status = 'done_loss'; outcome = 'loss'
    elif action == 'double':
        # Must have exactly 2 cards, and enough points for another bet
        if len(pcards) != 2:
            return jsonify({'error': 'Tuplaus vain ensimmäisellä vuorolla.'}), 400
        if _atomic_deduct_points(db, pid, bet, 'Blackjack tuplaus') is None:
            return jsonify({'error': 'Ei tarpeeksi pisteitä tuplaukseen.'}), 400
        bet *= 2
        pcards.append(deck.pop())
        if _hand_total(pcards) > 21:
            status = 'done_bust'; outcome = 'bust'
        else:
            while _hand_total(dcards) < 17:
                dcards.append(deck.pop())
            ptot, dtot = _hand_total(pcards), _hand_total(dcards)
            if dtot > 21 or ptot > dtot:
                status = 'done_win';  outcome = 'win';  payout = bet * 2
            elif ptot == dtot:
                status = 'done_push'; outcome = 'push'; payout = bet
            else:
                status = 'done_loss'; outcome = 'loss'
    elif action == 'insurance':
        if len(pcards) != 2:
            return jsonify({'error': 'Vakuutus on mahdollinen vain pelin alussa.'}), 400
        if dcards[0]['rank'] != 'A':
            return jsonify({'error': 'Vakuutus on mahdollinen vain kun jakajalla on ässä.'}), 400
        if game['insurance_bet'] and game['insurance_bet'] > 0:
            return jsonify({'error': 'Vakuutus on jo otettu.'}), 400
        ins = max(1, bet // 2)
        if _atomic_deduct_points(db, pid, ins, 'Blackjack vakuutuspanos') is None:
            return jsonify({'error': 'Ei tarpeeksi pisteitä vakuutukseen.'}), 400
        dealer_bj = _hand_total(dcards) == 21
        if dealer_bj:
            ins_payout = ins * 3  # 2:1 pays: get stake back + 2× profit
            _add_points(db, pid, ins_payout, 'Blackjack vakuutus voitto')
            if _hand_total(pcards) == 21:
                # Both have BJ — push on main hand
                _add_points(db, pid, bet, 'Blackjack tasapeli (BJ vs BJ)')
                status = 'done_push'; outcome = 'push'; payout = bet
            else:
                status = 'done_loss'; outcome = 'loss'; payout = 0
            # Apply streak override
            streak = _get_streak_mode(db, pid)
            if streak == 'win' and outcome == 'loss':
                # Force win: refund bet too
                _add_points(db, pid, bet * 2, 'Blackjack voitto (streak)')
                outcome = 'win'; status = 'done_win'; payout = bet * 2
            net_total = (ins_payout - ins) + (payout - bet)
        else:
            ins_payout = 0
            outcome    = None
            status     = 'active'
            net_total  = -ins
        db.execute(
            'UPDATE blackjack_games SET insurance_bet=?,status=? WHERE id=?',
            (ins, status, gid)
        )
        db.commit()
        game  = db.execute('SELECT * FROM blackjack_games WHERE id=?', (gid,)).fetchone()
        state = _bj_state(game)
        bal   = db.execute('SELECT points FROM players WHERE id=?', (pid,)).fetchone()['points'] or 0
        state['points']           = bal
        state['dealer_has_bj']    = dealer_bj
        state['insurance_result'] = 'win' if dealer_bj else 'loss'
        state['insurance_payout'] = ins_payout
        state['insurance_amount'] = ins
        if dealer_bj and outcome:
            state['outcome'] = outcome
            state['payout']  = payout
            state['net']     = net_total
        else:
            state['net'] = net_total
        return jsonify(state)
    else:
        return jsonify({'error': 'Virheellinen toiminto.'}), 400

    # Apply streak override when game ends
    if outcome is not None:
        streak = _get_streak_mode(db, pid)
        if streak == 'lose' and outcome in ('win', 'push'):
            outcome = 'loss'; status = 'done_loss'; payout = 0
        elif streak == 'win' and outcome in ('loss', 'bust'):
            outcome = 'win'; status = 'done_win'; payout = bet * 2
    if payout > 0:
        _add_points(db, pid, payout, f'Blackjack {outcome}')
    db.execute(
        '''UPDATE blackjack_games SET deck_json=?,player_cards_json=?,dealer_cards_json=?,status=?,bet=?
           WHERE id=?''',
        (json.dumps(deck), json.dumps(pcards), json.dumps(dcards), status, bet, gid)
    )
    db.commit()
    game = db.execute('SELECT * FROM blackjack_games WHERE id=?', (gid,)).fetchone()
    state = _bj_state(game)
    bal = db.execute('SELECT points FROM players WHERE id=?', (pid,)).fetchone()['points'] or 0
    state['points'] = bal
    if outcome:
        state['outcome'] = outcome
        state['payout']  = payout
        state['net']     = payout - bet  # bet is doubled if they doubled
    return jsonify(state)

# ─── Streak mode (admin) ─────────────────────────────────────────────────────

@app.route('/api/players/<int:pid>/streak', methods=['POST'])
def set_streak_mode(pid):
    d    = request.json or {}
    mode = d.get('mode', 'normal')
    if mode not in ('normal', 'win', 'lose'):
        return jsonify({'error': 'Virheellinen tila.'}), 400
    db = get_db()
    if not db.execute('SELECT id FROM players WHERE id=?', (pid,)).fetchone():
        return jsonify({'error': 'Pelaajaa ei löydy.'}), 404
    db.execute('UPDATE players SET streak_mode=? WHERE id=?', (mode, pid))
    db.commit()
    return jsonify({'ok': True, 'streak_mode': mode})

# ─── System settings (admin) ─────────────────────────────────────────────────

@app.route('/api/settings', methods=['GET'])
def get_settings():
    db   = get_db()
    rows = db.execute('SELECT key, value FROM system_settings').fetchall()
    out  = dict(SETTINGS_DEFAULTS)
    for r in rows:
        out[r['key']] = r['value']
    return jsonify(out)

@app.route('/api/settings', methods=['PUT'])
def update_settings():
    d  = request.json or {}
    db = get_db()
    for key, val in d.items():
        if key in SETTINGS_DEFAULTS:
            db.execute('INSERT OR REPLACE INTO system_settings(key,value) VALUES(?,?)',
                       (key, str(val)))
    db.commit()
    return jsonify({'ok': True})

# ─── Cash redemption (points → EUR bonus) ────────────────────────────────────

@app.route('/api/players/<int:pid>/points/cash-redeem', methods=['POST'])
def cash_redeem(pid):
    d  = request.json or {}
    db = get_db()
    try:
        pts = int(d.get('points', 0))
    except (TypeError, ValueError):
        return jsonify({'error': 'Virheellinen pisteiden määrä.'}), 400
    min_pts = int(_get_setting(db, 'min_redeem_pts'))
    max_pts = int(_get_setting(db, 'max_redeem_pts'))
    ppu     = float(_get_setting(db, 'points_per_eur'))   # points per €1
    if pts < min_pts:
        return jsonify({'error': f'Vähimmäislunastus on {min_pts} pistettä.'}), 400
    if pts > max_pts:
        return jsonify({'error': f'Enimmäislunastus on {max_pts} pistettä kerrallaan.'}), 400
    eur     = round(pts / ppu, 2)
    new_bal = _atomic_deduct_points(db, pid, pts, f'Käteisnosto: {pts} p → €{eur:.2f}')
    if new_bal is None:
        return jsonify({'error': 'Ei tarpeeksi pisteitä.'}), 400
    db.execute(
        'INSERT INTO bonuses(player_id,label,amount,seen) VALUES(?,?,?,?)',
        (pid, f'Pisteistä lunastettu: {pts} pistettä', float(eur), 0)
    )
    db.commit()
    return jsonify({'ok': True, 'points': new_bal, 'eur': eur, 'pts_redeemed': pts})

# ─── Pikapokeri (Jacks-or-Better video poker) ─────────────────────────────────

@app.route('/api/points/<int:pid>/pikapokeri/start', methods=['POST'])
def pikapokeri_start(pid):
    d  = request.json or {}
    db = get_db()
    db.execute("UPDATE pikapokeri_games SET status='abandoned' WHERE player_id=? AND status='deal'", (pid,))
    bet, err = _get_bet(d, pid, db)
    if err: return err
    _atomic_deduct_points(db, pid, bet, 'Pikapokeri panos')
    deck = new_deck()
    hand = [deck.pop() for _ in range(5)]
    cur  = db.execute(
        'INSERT INTO pikapokeri_games(player_id,bet,deck_json,hand_json,status) VALUES(?,?,?,?,?)',
        (pid, bet, json.dumps(deck), json.dumps(hand), 'deal')
    )
    gid = cur.lastrowid
    db.commit()
    bal = db.execute('SELECT points FROM players WHERE id=?', (pid,)).fetchone()['points'] or 0
    return jsonify({'game_id': gid, 'hand': hand, 'bet': bet, 'status': 'deal', 'points': bal})

@app.route('/api/points/pikapokeri/<int:gid>/draw', methods=['POST'])
def pikapokeri_draw(gid):
    d    = request.json or {}
    hold = [int(i) for i in d.get('hold', []) if str(i).isdigit()]
    db   = get_db()
    game = db.execute('SELECT * FROM pikapokeri_games WHERE id=?', (gid,)).fetchone()
    if not game:
        return jsonify({'error': 'Peliä ei löydy.'}), 404
    if game['status'] != 'deal':
        return jsonify({'error': 'Peli on jo päättynyt.'}), 400
    pid  = game['player_id']
    bet  = game['bet']
    deck = json.loads(game['deck_json'])
    hand = json.loads(game['hand_json'])

    new_hand = [hand[i] if i in hold else deck.pop() for i in range(5)]

    rank, mult, result_name = _pikapokeri_eval(new_hand)

    streak = _get_streak_mode(db, pid)
    if streak == 'lose' and mult > 0:
        rank, mult, result_name = -1, 0, 'Häviö'
    elif streak == 'win' and mult == 0:
        rank, mult, result_name = 3, 3, 'Kolmikko'

    payout = bet * mult
    if payout > 0:
        _add_points(db, pid, payout, f'Pikapokeri voitto ({result_name})')

    db.execute(
        'UPDATE pikapokeri_games SET hand_json=?,deck_json=?,status=?,payout=?,result_rank=?,result_name=? WHERE id=?',
        (json.dumps(new_hand), json.dumps(deck), 'done', payout, rank, result_name, gid)
    )
    db.commit()
    bal     = db.execute('SELECT points FROM players WHERE id=?', (pid,)).fetchone()['points'] or 0
    outcome = 'win' if payout > 0 else 'loss'
    return jsonify({
        'hand': new_hand, 'rank': rank, 'result_name': result_name,
        'mult': mult, 'bet': bet, 'payout': payout, 'net': payout - bet,
        'outcome': outcome, 'points': bal,
    })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
