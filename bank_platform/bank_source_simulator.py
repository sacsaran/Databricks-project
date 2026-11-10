# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Retail Bank: Source System Simulator (Customer 360 + Transactions)
# MAGIC
# MAGIC Generates realistic, *deliberately messy* data from 10 source systems into a Unity Catalog **Volume**,
# MAGIC as files that Auto Loader can ingest. Built for **Databricks Free Edition** (serverless, no Kafka needed).
# MAGIC
# MAGIC | Landing folder | Simulates | Format | Messiness planted |
# MAGIC |---|---|---|---|
# MAGIC | `customers_cdc/` | Core banking DB (Debezium-style CDC) | JSON | out-of-order, duplicates, deletes, late events, **schema evolution** (day 4+) |
# MAGIC | `accounts_cdc/` | Core banking DB (CDC) | JSON | same; delayed account rows -> **late-arriving dimension** |
# MAGIC | `transactions/` | Card switch stream (4-hour micro-batch files) | JSON | **late** (15min-3h), **very late** (6-72h), duplicates, dirty rows, fraud bursts |
# MAGIC | `clearing/` | Visa/Mastercard partner settlement | CSV | files days late, amount mismatches, later CORRECTION rows |
# MAGIC | `fraud_labels/` | Chargeback / analyst labels | JSON | arrive 1-5 days after the transaction |
# MAGIC | `fx_rates/` | FX REST API | JSON | weekends missing, some days delivered late |
# MAGIC | `sanctions_list/` | Watchlist (OFSI/OFAC/EU) | CSV | weekly full snapshot, contains exact + fuzzy matches to real customers |
# MAGIC | `crm_contacts/`, `crm_cases/` | Salesforce export | JSON | null/mismatched customer_id, upper-case emails, multiple versions per case, PII in free text |
# MAGIC | `clickstream/` | Mobile app events | JSON | offline-batched late events, new field `biometric_used` in newer app versions, account-takeover sessions |
# MAGIC
# MAGIC It also creates small Delta reference tables in `bank_gov` (persona map for row/column security, MCC reference, column classification)
# MAGIC and a `_truth/` folder with the **ground truth** so you can write reconciliation tests.
# MAGIC
# MAGIC **How to use:** run all cells once (creates a 7-day history). Then re-run the *"Simulate more days"* cell
# MAGIC whenever you want fresh files to arrive while your streaming pipeline is running.

# COMMAND ----------

# DBTITLE 1,Config
import json, os, csv, math, random, time
from datetime import datetime, timedelta, timezone, date

CATALOG = "bank_platform"          # Free Edition default catalog
SCHEMA = "bank_raw"            # schema that holds the landing volume
VOLUME = "landing"
GOV_SCHEMA = "bank_gov"        # reference / security tables

# SEED = 42
SEED = False                   # get this to get another bettwe data
N_CUSTOMERS = 5000
HISTORY_DAYS = 7
TXNS_PER_DAY = 25000           # ~175k transactions after 7 days. Raise on a paid workspace.
CLICKS_PER_DAY = 12000
SCHEMA_V2_DAY = 4              # from this simulated day, CDC events carry a new column (schema evolution)
RESET = True                   # True = wipe the landing volume and start from scratch

BASE = os.environ.get("BANK_SIM_BASE", f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}")
STATE_PATH = f"{BASE}/_state/state.json"

# COMMAND ----------

# DBTITLE 1,Create schema + volume (and optionally reset)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOV_SCHEMA}")

if RESET:
    for d in os.listdir(BASE):
        dbutils.fs.rm(f"{BASE}/{d}", True)
print("Landing zone:", BASE)

# COMMAND ----------

# DBTITLE 1,Reference data + helpers
FIRST = ["Oliver","Amelia","George","Isla","Harry","Ava","Noah","Mia","Jack","Emily","Leo","Sophia","Arthur","Grace",
         "Freddie","Lily","Oscar","Ruby","Archie","Chloe","Alfie","Ella","Theo","Evie","Aisha","Mohammed","Priya","Arjun",
         "Fatima","Hassan","Ngozi","Tunde","Wei","Mei","Piotr","Anna","Lucas","Sofia","Callum","Fiona"]
LAST = ["Smith","Jones","Taylor","Brown","Williams","Wilson","Johnson","Davies","Patel","Khan","Ali","Singh","Evans",
        "Thomas","Roberts","Walker","Wright","Robinson","Thompson","White","Hughes","Edwards","Green","Hall","Wood",
        "Harris","Lewis","Martin","Jackson","Clarke","Okafor","Nguyen","Kowalski","Murphy","Campbell","Stewart","Morgan"]
REGIONS = {
    "London": ["London","Croydon","Bromley","Harrow"],
    "South East": ["Brighton","Reading","Oxford","Southampton"],
    "North West": ["Manchester","Liverpool","Preston","Chester"],
    "North East & Yorkshire": ["Newcastle","Leeds","York","Sunderland"],
    "Midlands": ["Birmingham","Nottingham","Leicester","Coventry"],
    "Scotland": ["Edinburgh","Glasgow","Aberdeen","Dundee"],
    "Wales": ["Cardiff","Swansea","Newport"],
    "South West": ["Bristol","Exeter","Plymouth","Bath"],
}
REGION_NAMES = list(REGIONS)
REGION_W = [25, 13, 12, 11, 14, 9, 5, 11]
STREETS = ["High Street","Station Road","Church Lane","Park Avenue","Victoria Road","Mill Lane","King Street","Queens Road"]
SEGMENTS = ["MASS","AFFLUENT","PRIVATE","STUDENT","SME"]
SEG_W = [55, 20, 3, 12, 10]
LANGS = ["en","en","en","en","cy","pl","ur","fr"]
OWNERS = ["j.patel","m.okafor","s.nguyen","l.rossi"]
CASE_CATS = ["CARD_LOST","FRAUD_DISPUTE","COMPLAINT","FEES_QUERY","ACCOUNT_ACCESS","ADDRESS_CHANGE","MORTGAGE_ENQUIRY"]

# mcc: (category, merchants, median GBP amount, weight)
MCC = {
    5411: ("Grocery", ["Tesco","Sainsburys","Aldi","Co-op","Waitrose","Lidl"], 28, 22),
    5812: ("Restaurants", ["Pizza Express","Nandos","Wagamama","Local Bistro"], 24, 14),
    5814: ("Fast Food", ["McDonalds","KFC","Greggs","Subway"], 9, 6),
    5541: ("Fuel", ["Shell","BP","Esso","Asda Petrol"], 55, 7),
    5311: ("Department Stores", ["John Lewis","Marks & Spencer","Debenhams"], 45, 5),
    4111: ("Transport", ["TfL","Trainline","National Rail","Uber"], 12, 9),
    5732: ("Electronics", ["Currys","Apple Store","Argos"], 120, 3),
    4899: ("Subscriptions", ["Netflix","Spotify","Sky","Disney Plus"], 11, 7),
    5999: ("Online Retail", ["Amazon","eBay","ASOS","Etsy"], 35, 12),
    5912: ("Pharmacy", ["Boots","Superdrug","Lloyds Pharmacy"], 14, 4),
    7011: ("Hotels", ["Premier Inn","Hilton","Travelodge"], 140, 2),
    4511: ("Airlines", ["British Airways","easyJet","Ryanair"], 220, 1.5),
    6011: ("ATM", ["ATM Withdrawal"], 60, 8),
}
MCC_KEYS = list(MCC)
MCC_W = [MCC[k][3] for k in MCC_KEYS]
ONLINE_MCC = {4899, 5999, 4511}

FX_START = {"USD": 1.27, "EUR": 1.17, "JPY": 190.0, "AED": 4.66, "INR": 106.0, "CHF": 1.12, "TRY": 41.0}
CUR_COUNTRY = {"USD": ["US"], "EUR": ["FR","DE","ES","IT"], "JPY": ["JP"], "AED": ["AE"], "INR": ["IN"], "CHF": ["CH"], "TRY": ["TR"]}

CUST_COLS = ["customer_id","first_name","last_name","national_id","date_of_birth","email","phone","address_line","city",
             "region","postcode","segment","kyc_status","risk_rating","marketing_consent","created_at","updated_at"]
ACCT_COLS = ["account_id","customer_id","account_type","sort_code","account_number","iban","currency","status",
             "overdraft_limit","credit_limit","opened_date","updated_at"]

# stream -> (bucket hours per file, format, name of the arrival-timestamp column to expose or None)
STREAMS = {
    "customers_cdc": (24, "json", None), "accounts_cdc": (24, "json", None),
    "transactions": (4, "json", "published_ts"), "clearing": (24, "csv", None),
    "fraud_labels": (24, "json", None), "fx_rates": (24, "json", None),
    "sanctions_list": (24, "csv", None), "crm_contacts": (24, "json", None),
    "crm_cases": (24, "json", None), "clickstream": (6, "json", "received_ts"),
}
HOUR_W = [1,1,1,1,1,2,4,7,9,8,8,9,10,9,8,8,9,10,10,9,7,5,3,2]

# ---- time helpers (all naive UTC) ----
EPOCH = datetime(1970, 1, 1)
def to_ms(dt): return int((dt - EPOCH).total_seconds() * 1000)
def iso(dt): return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"
def parse(s): return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ")

# ---- generators ----
def ni_number(r):
    L = "ABCEGHJKLMNPRSTWXYZ"
    return f"{r.choice(L)}{r.choice(L)}{r.randint(0,99):02d}{r.randint(0,99):02d}{r.randint(0,99):02d}{r.choice('ABCD')}"

def luhn_pan(r, prefix):
    digits = [int(x) for x in prefix] + [r.randint(0, 9) for _ in range(15 - len(prefix))]
    total = 0
    for i, d in enumerate(reversed(digits)):
        if i % 2 == 0:
            d = d * 2
            d = d - 9 if d > 9 else d
        total += d
    digits.append((10 - total % 10) % 10)
    return "".join(map(str, digits))

def make_postcode(r):
    return f"{r.choice(['SW','E','N','M','B','G','EH','CF','BS','LS'])}{r.randint(1,20)} {r.randint(1,9)}{r.choice('ABDEFGHJLNPQRSTUWXYZ')}{r.choice('ABDEFGHJLNPQRSTUWXYZ')}"

def make_phone(r): return f"+44 7{r.randint(100,999)} {r.randint(100000,999999)}"

def write_file(path, rows, fmt="json"):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if fmt == "json":
        with open(path, "w") as f:
            for row in rows:
                f.write(json.dumps(row, separators=(",", ":")) + "\n")
    else:
        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)

def partial_shuffle(r, rows, frac=0.15):
    if len(rows) < 4: return
    idx = r.sample(range(len(rows)), int(len(rows) * frac))
    vals = [rows[i] for i in idx]
    r.shuffle(vals)
    for i, v in zip(idx, vals): rows[i] = v

# COMMAND ----------

# DBTITLE 1,Entity factories (customers, accounts, CRM, CDC envelope)
def new_customer(r, st, created, k):
    st["n_cust"] += 1
    region = r.choices(REGION_NAMES, REGION_W)[0]
    fn, ln = r.choice(FIRST), r.choice(LAST)
    dob = date(1945, 1, 1) + timedelta(days=r.randint(0, 365 * 58))
    c = {
        "customer_id": f"C{st['n_cust']:07d}", "first_name": fn, "last_name": ln,
        "national_id": ni_number(r), "date_of_birth": dob.isoformat(),
        "email": f"{fn}.{ln}{r.randint(1,999)}@example.com".lower(), "phone": make_phone(r),
        "address_line": f"{r.randint(1,250)} {r.choice(STREETS)}", "city": r.choice(REGIONS[region]),
        "region": region, "postcode": make_postcode(r), "segment": r.choices(SEGMENTS, SEG_W)[0],
        "kyc_status": r.choices(["VERIFIED","PENDING","EXPIRED"], [90, 7, 3])[0],
        "risk_rating": r.choices(["LOW","MEDIUM","HIGH"], [80, 17, 3])[0],
        "marketing_consent": r.random() < 0.6, "created_at": iso(created), "updated_at": iso(created),
    }
    if k >= SCHEMA_V2_DAY: c["preferred_language"] = r.choice(LANGS)
    st["customers"][c["customer_id"]] = c
    return c

def open_accounts(r, st, c, opened):
    types = ["CURRENT" if r.random() < 0.92 else "SAVINGS"]
    if r.random() < 0.35: types.append("SAVINGS")
    if r.random() < 0.25: types.append("CREDIT_CARD")
    if r.random() < 0.08: types.append("ISA")
    out = []
    for i, t in enumerate(types):
        st["n_acct"] += 1
        n = st["n_acct"]
        o = opened + timedelta(seconds=r.randint(30, 600) * (i + 1))
        sc = f"{r.randint(10,99)}-{r.randint(10,99)}-{r.randint(10,99)}"
        accno = f"{r.randint(10**7, 10**8 - 1)}"
        a = {
            "account_id": f"A{n:07d}", "customer_id": c["customer_id"], "account_type": t, "sort_code": sc,
            "account_number": accno, "iban": f"GB{r.randint(10,99)}FKBK{sc.replace('-','')}{accno}", "currency": "GBP",
            "status": "ACTIVE", "overdraft_limit": r.choice([0,250,500,1000,2000]) if t == "CURRENT" else 0,
            "credit_limit": r.choice([1000,3000,5000,10000]) if t == "CREDIT_CARD" else 0,
            "opened_date": o.date().isoformat(), "updated_at": iso(o), "_opened_ms": to_ms(o),
        }
        st["accounts"][a["account_id"]] = a
        if t in ("CURRENT", "CREDIT_CARD"):
            st["cards"][a["account_id"]] = {"card_id": f"CRD{n:08d}", "pan": luhn_pan(r, r.choice(["4", "5"]))}
        st["act"][a["account_id"]] = r.lognormvariate(0, 1.0)
        out.append((a, o))
    return out

def mutate_customer(r, c, ts, k):
    for ch in r.sample(["address","contact","segment","kyc","risk","consent"], r.randint(1, 2)):
        if ch == "address":
            c["region"] = r.choices(REGION_NAMES, REGION_W)[0]
            c["city"] = r.choice(REGIONS[c["region"]])
            c["address_line"] = f"{r.randint(1,250)} {r.choice(STREETS)}"
            c["postcode"] = make_postcode(r)
        elif ch == "contact":
            c["phone"] = make_phone(r)
            if r.random() < 0.5: c["email"] = f"{c['first_name']}.{c['last_name']}{r.randint(1,999)}@example.com".lower()
        elif ch == "segment": c["segment"] = r.choices(SEGMENTS, SEG_W)[0]
        elif ch == "kyc": c["kyc_status"] = r.choices(["VERIFIED","PENDING","EXPIRED"], [70, 15, 15])[0]
        elif ch == "risk": c["risk_rating"] = r.choices(["LOW","MEDIUM","HIGH"], [50, 35, 15])[0]
        else: c["marketing_consent"] = not c["marketing_consent"]
    if k >= SCHEMA_V2_DAY and not c.get("preferred_language"): c["preferred_language"] = r.choice(LANGS)
    c["updated_at"] = iso(ts)

def mutate_account(r, a, ts):
    if r.random() < 0.6 or a["account_type"] not in ("CURRENT", "CREDIT_CARD"):
        a["status"] = r.choices(["ACTIVE","DORMANT","FROZEN"], [60, 25, 15])[0]
    elif a["account_type"] == "CURRENT":
        a["overdraft_limit"] = r.choice([0,250,500,1000,2000])
    else:
        a["credit_limit"] = r.choice([1000,3000,5000,10000])
    a["updated_at"] = iso(ts)

def cdc_ev(table, op, rec, ts, cols, key):
    e = {"op": op, "source_table": table, "source_ts_ms": to_ms(ts)}
    for c in cols:
        e[c] = rec.get(c) if (op != "d" or c == key) else None
    return e

def cdc_delay(r):
    x = r.random()
    if x < 0.90: return timedelta(seconds=r.randint(1, 120))
    if x < 0.97: return timedelta(hours=r.randint(1, 30))
    return timedelta(hours=r.randint(30, 70))

def make_contact(r, st, c, ts):
    cid = c["customer_id"]
    ct = st["contacts"].get(cid)
    if ct is None:
        ct = {"crm_contact_id": f"003{len(st['contacts'])+1:09d}", "owner": r.choice(OWNERS)}
        st["contacts"][cid] = ct
    ct.update({
        "customer_id": cid if r.random() > 0.03 else None,            # 3% unlinked contacts
        "full_name": f"{c['first_name']} {c['last_name']}",
        "email": c["email"].upper() if r.random() < 0.05 else c["email"],  # 5% dirty casing
        "lifecycle_stage": r.choices(["Prospect","Customer","At-Risk","Churned"], [5, 80, 10, 5])[0],
        "nps_score": r.choice([None] + list(range(0, 11))),
        "preferred_channel": r.choice(["EMAIL","SMS","PHONE","APP"]), "SystemModstamp": iso(ts),
    })
    return dict(ct)

# COMMAND ----------

# DBTITLE 1,Transaction + clickstream factories
def make_txn(r, st, a, ts, fraud=False):
    st["n_txn"] += 1
    card = st["cards"][a["account_id"]]
    merchant, mcc, cat, country, city = None, None, None, "GB", r.choice(REGIONS[r.choice(REGION_NAMES)])
    cur = "GBP"
    if fraud:
        mcc = r.choice([5999, 5732, 4511, 7011]); cur = r.choice(["USD","EUR","AED","TRY"])
        ttype, channel, gbp = "PURCHASE", "ECOM", r.uniform(300, 2500)
        cat, merchant = MCC[mcc][0], r.choice(MCC[mcc][1])
        country = r.choice(CUR_COUNTRY[cur])
    else:
        x = r.random()
        if x < 0.05:
            ttype, channel, gbp, merchant, cat = "TRANSFER_OUT", r.choice(["MOBILE","ONLINE_BANKING"]), r.lognormvariate(math.log(150), 0.9), "Faster Payments", "Transfer"
        elif x < 0.08:
            ttype, channel, gbp, merchant, cat = "TRANSFER_IN", r.choice(["MOBILE","ONLINE_BANKING"]), r.lognormvariate(math.log(250), 0.9), "Faster Payments", "Transfer"
        else:
            mcc = r.choices(MCC_KEYS, MCC_W)[0]
            cat, ms, med, _ = MCC[mcc]
            merchant = r.choice(ms)
            gbp = r.lognormvariate(math.log(med), 0.8)
            ttype = "ATM_WITHDRAWAL" if mcc == 6011 else ("REFUND" if r.random() < 0.03 else "PURCHASE")
            channel = "ATM" if mcc == 6011 else ("ECOM" if (mcc in ONLINE_MCC or r.random() < 0.12) else "POS")
            if mcc != 6011 and r.random() < 0.06:
                cur = r.choice(list(FX_START)); country = r.choice(CUR_COUNTRY[cur])
    amount = gbp * (st["fx"][cur] if cur != "GBP" else 1.0)
    status = "AUTHORISED" if fraud else r.choices(["AUTHORISED","DECLINED","REVERSED"], [94, 5, 1])[0]
    is_card = ttype in ("PURCHASE", "REFUND", "ATM_WITHDRAWAL")
    return {
        "transaction_id": f"T{st['n_txn']:010d}", "account_id": a["account_id"],
        "card_id": card["card_id"] if is_card else None, "card_number": card["pan"] if is_card else None,
        "txn_type": ttype, "direction": "CREDIT" if ttype in ("TRANSFER_IN", "REFUND") else "DEBIT",
        "amount": round(amount, 2), "currency": cur, "merchant_name": merchant, "mcc": mcc, "merchant_category": cat,
        "channel": channel, "merchant_city": city if country == "GB" else None, "country": country,
        "status": status, "event_ts": iso(ts),
    }

def txn_delay(r):
    x = r.random()
    if x < 0.90: return timedelta(seconds=r.randint(1, 25)), "ontime"
    if x < 0.985: return timedelta(minutes=r.randint(15, 180)), "late"
    return timedelta(hours=r.randint(6, 72)), "very_late"

FLOWS = [["app_open","login_success","view_balance"],
         ["app_open","login_success","view_balance","view_transactions"],
         ["app_open","login_success","view_balance","transfer_start","transfer_complete"],
         ["app_open","login_failed","login_success","view_balance"],
         ["app_open","login_success","card_freeze"],
         ["app_open","login_success","view_transactions","card_unfreeze"]]
FLOW_W = [35, 25, 15, 10, 5, 10]
ATO_FLOW = ["app_open","login_failed","login_failed","login_failed","password_reset","login_success",
            "change_address","transfer_start","transfer_complete"]

# COMMAND ----------

# DBTITLE 1,Stream flushing (turns "pending events" into landed files)
def flush_stream(r, st, stream, new_events, day_end):
    """Everything whose delivery time falls before day_end is written to files; the rest stays pending (= late data)."""
    hours, fmt, expose = STREAMS[stream]
    de = iso(day_end)
    pend = st["pending"].get(stream, []) + new_events
    due = [e for e in pend if e["_deliver"] < de]
    st["pending"][stream] = [e for e in pend if e["_deliver"] >= de]
    buckets = {}
    for e in due:
        dt = parse(e["_deliver"])
        b = dt.replace(hour=(dt.hour // hours) * hours, minute=0, second=0, microsecond=0)
        buckets.setdefault(b, []).append(e)
    n = 0
    for b, evs in sorted(buckets.items()):
        evs.sort(key=lambda e: e["_deliver"])
        partial_shuffle(r, evs)
        rows = []
        for e in evs:
            row = {k: v for k, v in e.items() if not k.startswith("_")}
            if expose: row[expose] = e["_deliver"]
            rows.append(row)
        suffix = f"{b:%Y%m%d}" if hours == 24 else f"{b:%Y%m%d_%H}"
        write_file(f"{BASE}/{stream}/{stream}_{suffix}.{fmt}", rows, fmt)
        n += len(rows)
    return n

# COMMAND ----------

# DBTITLE 1,Initial state + day-0 snapshot
def init_state():
    r = random.Random(SEED)
    start = datetime.now(timezone.utc).replace(tzinfo=None, hour=0, minute=0, second=0, microsecond=0) - timedelta(days=HISTORY_DAYS)
    st = {"start": iso(start), "last_day": 0, "lsn": 0, "n_cust": 0, "n_acct": 0, "n_txn": 0, "n_click": 0, "n_case": 0,
          "n_sanc": 0, "customers": {}, "accounts": {}, "cards": {}, "act": {}, "contacts": {}, "cases": {},
          "pending": {}, "stats": {}, "fx": dict(FX_START), "sanctions": []}
    snap_ts = start - timedelta(seconds=1)
    cust_ev, acct_ev, contact_rows = [], [], []
    for _ in range(N_CUSTOMERS):
        created = start - timedelta(days=r.randint(30, 1100), seconds=r.randint(0, 86399))
        c = new_customer(r, st, created, 0)
        st["lsn"] += 1
        e = cdc_ev("customers", "r", c, snap_ts, CUST_COLS, "customer_id"); e["lsn"] = st["lsn"]; cust_ev.append(e)
        for a, _o in open_accounts(r, st, c, created):
            st["lsn"] += 1
            e = cdc_ev("accounts", "r", a, snap_ts, ACCT_COLS, "account_id"); e["lsn"] = st["lsn"]; acct_ev.append(e)
        contact_rows.append(make_contact(r, st, c, snap_ts))
    tag = f"{start:%Y%m%d}"
    write_file(f"{BASE}/customers_cdc/customers_cdc_{tag}_snapshot.json", cust_ev)
    write_file(f"{BASE}/accounts_cdc/accounts_cdc_{tag}_snapshot.json", acct_ev)
    write_file(f"{BASE}/crm_contacts/crm_contacts_{tag}_snapshot.json", contact_rows)

    # sanctions watchlist: random foreign entities + 12 planted matches to real customers
    foreign_f = ["Ivan","Dmitri","Olga","Viktor","Hamid","Reza","Tomas","Nikolai","Karim","Sergei"]
    foreign_l = ["Petrov","Volkov","Sokolov","Karimi","Rahimi","Novak","Morozov","Orlov","Haddad","Zaitsev"]
    def sanc(name, dob):
        st["n_sanc"] += 1
        return {"entity_id": f"SL-{st['n_sanc']:06d}", "full_name": name, "date_of_birth": dob,
                "nationality": r.choice(["RU","IR","SY","KP","BY","MM"]), "list_source": r.choice(["OFSI","OFAC","EU"]),
                "programme": r.choice(["Russia","Terrorism","Cyber","Human rights"]),
                "listed_date": (start.date() - timedelta(days=r.randint(30, 2000))).isoformat()}
    for _ in range(300):
        st["sanctions"].append(sanc(f"{r.choice(foreign_f)} {r.choice(foreign_l)}",
                                    (date(1950, 1, 1) + timedelta(days=r.randint(0, 20000))).isoformat()))
    hits = r.sample(list(st["customers"].values()), 12)
    for i, c in enumerate(hits):
        name = f"{c['first_name']} {c['last_name']}".title()
        if i >= 6: name = name + c["last_name"][-1]            # fuzzy match, e.g. 'Smithh'
        st["sanctions"].append(sanc(name, c["date_of_birth"]))
    st["stats"]["sanctions_planted_matches"] = 12
    st["stats"]["customers_snapshot"] = N_CUSTOMERS
    st["stats"]["accounts_snapshot"] = len(st["accounts"])
    return st

# COMMAND ----------

# DBTITLE 1,simulate_day(): one day of activity across all sources
def simulate_day(st, k):
    r = random.Random(SEED * 1000 + k)
    day_start = parse(st["start"]) + timedelta(days=k - 1)
    day_end = day_start + timedelta(days=1)
    S = st["stats"]
    def bump(key, n=1): S[key] = S.get(key, 0) + n
    def rand_ts(): return day_start + timedelta(seconds=r.randint(0, 86399), milliseconds=r.randint(0, 999))
    def crm_deliver(ts):
        d = max(ts + timedelta(minutes=5), day_end - timedelta(hours=1))
        if r.random() < 0.03: d += timedelta(days=r.randint(1, 2))
        return iso(d)
    new_ev = {s: [] for s in STREAMS}

    # ---------- 1. Core banking CDC (customers + accounts) ----------
    cols_c = CUST_COLS + (["preferred_language"] if k >= SCHEMA_V2_DAY else [])
    live = [c for c in st["customers"].values() if not c.get("_deleted")]
    n_upd, n_del, n_new = int(len(live) * 0.02), max(1, int(len(live) * 0.002)), int(len(live) * 0.01) + r.randint(0, 8)
    picked = r.sample(live, n_upd + n_del)
    to_del, to_upd = picked[:n_del], picked[n_del:]
    live_accts = [a for a in st["accounts"].values() if a["status"] != "CLOSED"]
    actions = [(rand_ts(), "upd_c", c["customer_id"]) for c in to_upd]
    actions += [(rand_ts(), "upd_c", c["customer_id"]) for c in r.sample(to_upd, int(len(to_upd) * 0.15))]  # 2nd update same day
    actions += [(rand_ts(), "del_c", c["customer_id"]) for c in to_del]
    actions += [(rand_ts(), "new_c", None) for _ in range(n_new)]
    actions += [(rand_ts(), "upd_a", a["account_id"]) for a in r.sample(live_accts, int(len(live_accts) * 0.015))]
    actions.sort(key=lambda x: x[0])

    cdc, contact_targets = [], []
    for ts, kind, ref in actions:
        if kind == "upd_c":
            c = st["customers"][ref]; mutate_customer(r, c, ts, k)
            cdc.append((ts, "customers", cdc_ev("customers", "u", c, ts, cols_c, "customer_id"))); bump("cdc_customer_updates")
        elif kind == "del_c":
            c = st["customers"][ref]; c["_deleted"] = True
            for a in [a for a in st["accounts"].values() if a["customer_id"] == ref and a["status"] != "CLOSED"]:
                a["status"] = "CLOSED"; a["updated_at"] = iso(ts + timedelta(seconds=1))
                cdc.append((ts + timedelta(seconds=1), "accounts", cdc_ev("accounts", "u", a, ts + timedelta(seconds=1), ACCT_COLS, "account_id")))
            cdc.append((ts + timedelta(seconds=2), "customers", cdc_ev("customers", "d", c, ts + timedelta(seconds=2), cols_c, "customer_id")))
            bump("cdc_customer_deletes")
        elif kind == "new_c":
            c = new_customer(r, st, ts, k)
            cdc.append((ts, "customers", cdc_ev("customers", "c", c, ts, cols_c, "customer_id")))
            for a, o in open_accounts(r, st, c, ts):
                cdc.append((o, "accounts", cdc_ev("accounts", "c", a, o, ACCT_COLS, "account_id")))
            contact_targets.append((c, ts)); bump("cdc_new_customers")
        elif kind == "upd_a":
            a = st["accounts"][ref]
            if a["status"] == "CLOSED": continue
            mutate_account(r, a, ts)
            cdc.append((ts, "accounts", cdc_ev("accounts", "u", a, ts, ACCT_COLS, "account_id"))); bump("cdc_account_updates")
    cdc.sort(key=lambda x: x[0])
    for ts, tbl, e in cdc:
        st["lsn"] += 1; e["lsn"] = st["lsn"]
        d = cdc_delay(r)
        e["_deliver"] = iso(ts + d)
        new_ev[f"{tbl}_cdc"].append(e); bump("cdc_events_unique")
        if r.random() < 0.02:
            dup = dict(e); dup["_deliver"] = iso(ts + d + timedelta(minutes=r.randint(1, 1500)))
            new_ev[f"{tbl}_cdc"].append(dup); bump("cdc_duplicates_emitted")

    # ---------- 2. Salesforce CRM ----------
    for c in r.sample(live, int(len(live) * 0.015)):
        if not c.get("_deleted"): contact_targets.append((c, rand_ts()))
    for c, ts in contact_targets:
        row = make_contact(r, st, c, ts); row["_deliver"] = crm_deliver(ts); new_ev["crm_contacts"].append(row)
    for cs in st["cases"].values():                           # progress open cases -> multiple versions per case_id
        if cs["status"] != "CLOSED" and r.random() < 0.35:
            ts = rand_ts()
            if ts <= parse(cs["SystemModstamp"]): ts = parse(cs["SystemModstamp"]) + timedelta(hours=1)
            cs["status"] = "OPEN" if cs["status"] == "NEW" else "CLOSED"
            if cs["status"] == "CLOSED": cs["closed_at"] = iso(ts)
            cs["SystemModstamp"] = iso(ts)
            row = dict(cs); row["_deliver"] = crm_deliver(ts); new_ev["crm_cases"].append(row)
    for _ in range(int(len(live) * 0.012)):
        c, ts = r.choice(live), rand_ts()
        st["n_case"] += 1
        cat = r.choice(CASE_CATS)
        cs = {"case_id": f"CS-{st['n_case']:07d}", "customer_id": c["customer_id"] if r.random() > 0.02 else None,
              "contact_email": c["email"], "category": cat, "priority": r.choices(["LOW","MEDIUM","HIGH"], [50, 40, 10])[0],
              "status": "NEW", "subject": f"{cat.replace('_',' ').title()} - {c['last_name']}",
              "description": f"Customer {c['first_name']} {c['last_name']} called from {c['phone']} about {cat.lower()}. "
                             f"Verified DOB {c['date_of_birth']}.",
              "opened_at": iso(ts), "closed_at": None, "SystemModstamp": iso(ts)}
        st["cases"][cs["case_id"]] = cs
        row = dict(cs); row["_deliver"] = crm_deliver(ts); new_ev["crm_cases"].append(row)
        bump("crm_cases_created")

    # ---------- 3. FX rates (REST API, weekdays only) ----------
    for cur in st["fx"]: st["fx"][cur] *= (1 + r.gauss(0, 0.004))
    if day_start.weekday() < 5:
        late = r.random() < 0.10
        for cur, v in st["fx"].items():
            new_ev["fx_rates"].append({"rate_date": day_start.date().isoformat(), "base_currency": "GBP", "quote_currency": cur,
                                       "rate": round(v, 5), "source": "SIM-ECB", "fetched_ts": iso(day_start + timedelta(hours=7)),
                                       "_deliver": iso(day_start + timedelta(hours=7) + (timedelta(days=1) if late else timedelta(0)))})

    # ---------- 4. Sanctions list (weekly full snapshot) ----------
    if k == 1 or (k - 1) % 7 == 0:
        if k > 1:
            for _ in range(5):
                st["n_sanc"] += 1
                st["sanctions"].append({"entity_id": f"SL-{st['n_sanc']:06d}", "full_name": f"{r.choice(FIRST)} {r.choice(LAST)}",
                                        "date_of_birth": (date(1950, 1, 1) + timedelta(days=r.randint(0, 20000))).isoformat(),
                                        "nationality": "RU", "list_source": "OFSI", "programme": "Russia", "listed_date": day_start.date().isoformat()})
            for _ in range(2): st["sanctions"].pop(r.randint(0, 299))
        for s in st["sanctions"]:
            row = dict(s); row["snapshot_date"] = day_start.date().isoformat(); row["_deliver"] = iso(day_start + timedelta(hours=8))
            new_ev["sanctions_list"].append(row)

    # ---------- 5. Card transactions (+ fraud, dirty data, lateness, duplicates) ----------
    pool = [a for a in st["accounts"].values() if a["account_id"] in st["cards"] and a["status"] in ("ACTIVE", "DORMANT")]
    wts = [st["act"][a["account_id"]] * (0.1 if a["status"] == "DORMANT" else 1.0) for a in pool]
    def fix_ts(a, ts):
        o = parse(iso(datetime(1970, 1, 1) + timedelta(milliseconds=a["_opened_ms"])))
        if ts < o: ts = o + timedelta(seconds=r.randint(60, 7200))
        return min(ts, day_end - timedelta(seconds=1))
    all_t = []   # (txn, ts, is_fraud)
    for a in r.choices(pool, weights=wts, k=TXNS_PER_DAY):
        h = r.choices(range(24), HOUR_W)[0]
        ts = fix_ts(a, day_start + timedelta(hours=h, seconds=r.randint(0, 3599), milliseconds=r.randint(0, 999)))
        all_t.append((make_txn(r, st, a, ts), ts, False))
    compromised = []
    for a in r.sample(pool, max(2, int(len(pool) * 0.002))):
        t0 = fix_ts(a, rand_ts())
        for i in range(r.randint(5, 14)):
            ts = min(t0 + timedelta(minutes=i * r.randint(1, 3), seconds=r.randint(0, 59)), day_end - timedelta(seconds=1))
            all_t.append((make_txn(r, st, a, ts, fraud=True), ts, True))
        compromised.append((a["customer_id"], t0))

    for t, ts, is_fraud in all_t:
        dirty = False
        if not is_fraud and r.random() < 0.004:                  # dirty records for data-quality expectations
            dirty = True; bump("txn_dirty")
            kind = r.choice(["null_account", "negative_amount", "bad_currency", "future_ts"])
            if kind == "null_account": t["account_id"] = None
            elif kind == "negative_amount" and t["txn_type"] == "PURCHASE": t["amount"] = -abs(t["amount"])
            elif kind == "bad_currency": t["currency"] = "XXX"
            else: t["event_ts"] = iso(ts + timedelta(days=400))
        delay, bucket = txn_delay(r)
        deliver = ts + delay
        bump("txn_unique"); bump(f"txn_{bucket}")
        if is_fraud: bump("txn_fraud")
        row = dict(t); row["_deliver"] = iso(deliver); new_ev["transactions"].append(row)
        if r.random() < 0.015:                                   # exact duplicate re-delivery
            dup = dict(t); dup["_deliver"] = iso(deliver + timedelta(minutes=r.randint(1, 360)))
            new_ev["transactions"].append(dup); bump("txn_duplicates_emitted")
        # partner clearing records (card txns only), usually next morning, sometimes days late
        if (not dirty and t["status"] == "AUTHORISED" and t["channel"] in ("POS", "ECOM") and r.random() < 0.85):
            amt = t["amount"]
            if r.random() < 0.80: d = day_end + timedelta(hours=r.randint(2, 6))
            else: d = day_end + timedelta(days=r.randint(1, 4), hours=r.randint(2, 6))
            mismatch = r.random() < 0.015
            settled = round(amt * r.uniform(0.9, 1.1), 2) if mismatch else amt
            base = {"transaction_id": t["transaction_id"], "scheme": "VISA" if t["card_number"][0] == "4" else "MASTERCARD",
                    "settlement_currency": t["currency"], "settlement_date": (ts + timedelta(days=1)).date().isoformat()}
            new_ev["clearing"].append({"record_type": "SETTLEMENT", **base, "settled_amount": settled,
                                       "scheme_fee": round(amt * 0.0015 + 0.02, 2), "_deliver": iso(d)})
            bump("clearing_rows")
            if mismatch:
                bump("clearing_mismatches")
                new_ev["clearing"].append({"record_type": "CORRECTION", **base, "settled_amount": amt,
                                           "scheme_fee": round(amt * 0.0015 + 0.02, 2),
                                           "_deliver": iso(d + timedelta(days=r.randint(3, 8)))})
        # fraud labels arrive days later (chargebacks / analyst review)
        if is_fraud and r.random() < 0.70:
            new_ev["fraud_labels"].append({"transaction_id": t["transaction_id"], "label": "FRAUD",
                                           "source": r.choice(["CHARGEBACK","ANALYST_REVIEW","CUSTOMER_REPORT"]),
                                           "confirmed_ts": iso(ts + timedelta(days=r.randint(1, 5))),
                                           "_deliver": iso(ts + timedelta(days=r.randint(1, 5), hours=r.randint(1, 12)))})
            bump("fraud_labels")
    for t, ts, is_fraud in r.sample([x for x in all_t if not x[2]], 15):          # analyst-cleared false positives
        new_ev["fraud_labels"].append({"transaction_id": t["transaction_id"], "label": "NOT_FRAUD", "source": "ANALYST_REVIEW",
                                       "confirmed_ts": iso(ts + timedelta(days=2)), "_deliver": iso(ts + timedelta(days=2, hours=3))})

    # ---------- 6. Mobile clickstream ----------
    live_now = [c for c in st["customers"].values() if not c.get("_deleted")]
    app_users = [c for c in live_now if int(c["customer_id"][1:]) % 10 < 6]
    def session(c, flow, t0, device, city, country, big=False):
        ver = r.choices(["5.1.0", "5.2.0", "5.3.1"], [20, 30, 50])[0]
        os_ = r.choice(["iOS", "Android"]); sid = f"S{st['n_click']:09d}x{r.randint(0,9999):04d}"
        ip = f"{r.randint(81,92)}.{r.randint(0,255)}.{r.randint(0,255)}.{r.randint(1,254)}"
        t = t0
        for et in flow:
            st["n_click"] += 1; t += timedelta(seconds=r.randint(3, 90))
            props = {}
            if et.startswith("transfer"): props = {"amount": round(r.uniform(2000, 9000) if big else r.lognormvariate(5, 1), 2), "payee_type": "NEW" if big else r.choice(["NEW","EXISTING"])}
            elif et.startswith("view"): props = {"screen": et}
            ev = {"event_id": f"E{st['n_click']:010d}", "session_id": sid, "customer_id": c["customer_id"] if c else None,
                  "device_id": device, "event_type": et, "event_ts": iso(t), "app_version": ver, "os": os_, "ip_address": ip,
                  "geo": {"city": city, "country": country}, "properties": props}
            if ver >= "5.2.0" and et.startswith("login"): ev["biometric_used"] = r.random() < 0.6   # newer app versions only
            x = r.random()
            dl = timedelta(seconds=r.randint(1, 60)) if x < 0.88 else (timedelta(hours=r.randint(1, 6)) if x < 0.97 else timedelta(hours=r.randint(24, 48)))
            ev["_deliver"] = iso(t + dl); new_ev["clickstream"].append(ev); bump("click_events")
    for _ in range(CLICKS_PER_DAY // 4):
        h = r.choices(range(24), HOUR_W)[0]
        t0 = day_start + timedelta(hours=h, seconds=r.randint(0, 3599))
        if r.random() < 0.06:
            session(None, ["app_open", "login_failed"], t0, f"DV{r.randint(0,99999):05d}x", r.choice(["London","Leeds"]), "GB")
        else:
            c = r.choice(app_users)
            session(c, r.choices(FLOWS, FLOW_W)[0], t0, f"DV{int(c['customer_id'][1:]) * 7919 % 100000:05d}{r.choice('ab')}", c["city"], "GB")
    for cid, t0 in compromised:                                  # account-takeover sessions just before the card fraud
        if r.random() < 0.5 and cid in st["customers"]:
            session(st["customers"][cid], ATO_FLOW, max(day_start, t0 - timedelta(minutes=40)), f"DV{r.randint(0,99999):05d}z",
                    "Bucharest", "RO", big=True)
            bump("ato_sessions")

    # ---------- 7. land the files ----------
    landed = {s: flush_stream(r, st, s, evs, day_end) for s, evs in new_ev.items()}
    st["last_day"] = k
    print(f"day {k} ({day_start:%Y-%m-%d}): " + ", ".join(f"{s}={n}" for s, n in landed.items() if n))

# COMMAND ----------

# DBTITLE 1,State + ground-truth persistence
def save_state(st):
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(STATE_PATH, "w") as f: json.dump(st, f)

def write_truth(st):
    """Ground truth = what the source systems look like *right now* (events still 'in flight' are listed under pending)."""
    cc = CUST_COLS + ["preferred_language"]
    write_file(f"{BASE}/_truth/customers_current.json", [{c: v.get(c) for c in cc} for v in st["customers"].values() if not v.get("_deleted")])
    write_file(f"{BASE}/_truth/accounts_current.json", [{c: v.get(c) for c in ACCT_COLS} for v in st["accounts"].values()])
    summary = dict(st["stats"])
    summary["days_simulated"] = st["last_day"]
    summary["customers_active"] = sum(1 for v in st["customers"].values() if not v.get("_deleted"))
    summary["pending_in_flight"] = {k: len(v) for k, v in st["pending"].items() if v}
    with open(f"{BASE}/_truth/summary.json", "w") as f: json.dump(summary, f, indent=2)

def add_days(n=1):
    """Simulate n more days. Re-run this while your streaming pipeline is running to feed it new files."""
    st = json.load(open(STATE_PATH))
    for _ in range(n): simulate_day(st, st["last_day"] + 1)
    save_state(st); write_truth(st)

# COMMAND ----------

# DBTITLE 1,Run: build the initial 7-day history
t0 = time.time()
state = init_state()
for k in range(1, HISTORY_DAYS + 1):
    simulate_day(state, k)
save_state(state); write_truth(state)
print(f"\nDone in {time.time() - t0:.0f}s")

# COMMAND ----------

# DBTITLE 1,Reference + security tables (Delta)
me = spark.sql("SELECT current_user()").first()[0]

# Free Edition has one user, so personas are simulated: exactly ONE row per user has is_active = true.
# Your row filter / column mask functions will read the active persona. Switch with the UPDATE shown below.
spark.createDataFrame([
    (me, "analyst", "London", False),
    (me, "analyst", "Scotland", False),
    (me, "fraud_investigator", "ALL", False),
    (me, "data_scientist", "ALL", False),
    (me, "compliance", "ALL", True),
], "user_email string, persona string, region string, is_active boolean") \
    .write.mode("overwrite").saveAsTable(f"{CATALOG}.{GOV_SCHEMA}.user_access_map")

spark.createDataFrame([(k, v[0]) for k, v in MCC.items()], "mcc int, category string") \
    .write.mode("overwrite").saveAsTable(f"{CATALOG}.{GOV_SCHEMA}.mcc_reference")

spark.createDataFrame([
    ("customers", "national_id", "PII_HIGH"), ("customers", "date_of_birth", "PII_HIGH"), ("customers", "email", "PII"),
    ("customers", "phone", "PII"), ("customers", "address_line", "PII"), ("customers", "postcode", "PII"),
    ("accounts", "iban", "CONFIDENTIAL"), ("accounts", "account_number", "CONFIDENTIAL"),
    ("transactions", "card_number", "PCI"), ("crm_cases", "description", "PII_FREE_TEXT"),
    ("clickstream", "ip_address", "PII"),
], "table_name string, column_name string, classification string") \
    .write.mode("overwrite").saveAsTable(f"{CATALOG}.{GOV_SCHEMA}.column_classification")

print("Created tables in", f"{CATALOG}.{GOV_SCHEMA}")
print("To switch persona later, e.g.:")
print(f"  UPDATE {CATALOG}.{GOV_SCHEMA}.user_access_map SET is_active = (persona = 'analyst' AND region = 'London') WHERE user_email = current_user()")

# COMMAND ----------

# DBTITLE 1,Verify: what landed?
for d in sorted(os.listdir(BASE)):
    if d.startswith("_"): continue
    files = os.listdir(f"{BASE}/{d}")
    mb = sum(os.path.getsize(f"{BASE}/{d}/{f}") for f in files) / 1e6
    print(f"{d:16s} {len(files):4d} files {mb:8.2f} MB")
print("\nGround truth summary:")
print(open(f"{BASE}/_truth/summary.json").read())

# COMMAND ----------

# DBTITLE 1,Peek: CDC operations and late transactions
cdc = spark.read.json(f"{BASE}/customers_cdc/")
display(cdc.groupBy("op").count())

from pyspark.sql import functions as F
tx = spark.read.json(f"{BASE}/transactions/")
lat = (tx.withColumn("lateness_min", (F.unix_timestamp("published_ts") - F.unix_timestamp("event_ts")) / 60)
         .withColumn("bucket", F.when(F.col("lateness_min") < 1, "on time (<1 min)")
                                .when(F.col("lateness_min") < 240, "late (<4h)").otherwise("very late (>4h)")))
display(lat.groupBy("bucket").count())

# COMMAND ----------

# DBTITLE 1,Simulate more days (re-run to drip-feed new files into a running stream)
add_days(1)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # **NOTE**
# MAGIC - To get different data, change SEED. With the same SEED = 42, you get the same customers, amounts and patterns each time. Only the dates shift, because the history is anchored to today. Set SEED to another number (say 7) for a genuinely new dataset.
# MAGIC - 	To add data without wiping, don’t Run all. Set RESET = False and run only the last cell (add_days(1)), which appends a new day of files.
# MAGIC - Downstream tables aren’t cleared. The reset only touches the landing volume. If you’ve already built Bronze or Silver tables, they’ll still hold the old data. Drop them as well, and delete your Auto Loader/stream checkpoints. Otherwise the stream thinks it has already processed those files and may fail or skip the new ones.
# MAGIC - The bank_gov tables are overwritten. That includes your persona settings, so any UPDATE you ran on user_access_map will be reset