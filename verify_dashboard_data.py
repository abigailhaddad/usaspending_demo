#!/usr/bin/env python3
"""
Independent check of data/reap_dashboard_data.json: recomputes the headline figures with DuckDB
straight from the parquet files (no pandas, none of the dashboard script's code) and compares.
Requires: pip install duckdb.   Usage: python verify_dashboard_data.py
Exit code 1 if anything disagrees by more than a cent.
"""
import json, sys
import duckdb

con = duckdb.connect()
con.execute("CREATE VIEW t AS SELECT * FROM read_parquet('data/reap_fy*_awards.parquet', union_by_name=true)")
con.execute("""CREATE VIEW a AS SELECT assistance_award_unique_key k,
    sum(federal_action_obligation) net, max(total_outlayed_amount_for_overall_award) outl,
    min(action_date) FILTER (WHERE federal_action_obligation > 0) first_pos
  FROM t GROUP BY 1""")
J = json.load(open('data/reap_dashboard_data.json'))
S = J['summary']
q1 = lambda sql: con.execute(sql).fetchone()[0]
rows, bad = [], []

def chk(name, json_val, indep_val, tol=0.01):
    ok = abs(float(json_val) - float(indep_val)) <= tol
    rows.append((name, json_val, indep_val, 'ok' if ok else 'MISMATCH'))
    if not ok: bad.append(name)

chk('unique awards', S['unique_awards'], q1("SELECT count(DISTINCT assistance_award_unique_key) FROM t"))
chk('transaction rows', S['total_transaction_rows'], q1("SELECT count(*) FROM t"))
chk('rows with non-zero obligation', S['total_transactions'], q1("SELECT count(*) FROM t WHERE federal_action_obligation <> 0"))
chk('zero-obligation rows', S['zero_obligation_rows'], q1("SELECT count(*) FROM t WHERE federal_action_obligation = 0"))
chk('total obligated', S['total_obligated'], q1("SELECT sum(federal_action_obligation) FROM t WHERE federal_action_obligation > 0"))
chk('total deobligated', S['total_deobligated'], q1("SELECT sum(federal_action_obligation) FROM t WHERE federal_action_obligation < 0"))
chk('net obligations', S['net_obligations'], q1("SELECT sum(federal_action_obligation) FROM t"))
outl = q1("SELECT sum(outl) FROM a")
chk('total outlays (once per award)', S['total_outlays'], outl)
chk('outlay rate %', S['outlay_rate'], outl / q1("SELECT sum(federal_action_obligation) FROM t") * 100, 1e-6)
chk('awards without positive obligation', S['awards_without_positive_obligation'], q1("SELECT count(*) FROM a WHERE first_pos IS NULL"))
chk('outlays on those awards', S['outlays_on_awards_without_positive_obligation'], q1("SELECT sum(outl) FROM a WHERE first_pos IS NULL"))
chk('awards with outlay reported', S['awards_with_outlay_reported'], q1("SELECT count(*) FROM a WHERE outl IS NOT NULL"))

# per state (an award counts once per state it appears under)
st = dict(con.execute("""SELECT recipient_state_code, sum(o) FROM (SELECT recipient_state_code, assistance_award_unique_key,
    max(total_outlayed_amount_for_overall_award) o FROM t GROUP BY 1,2) GROUP BY 1""").fetchall())
js = {r['recipient_state_code']: r for r in J['state_data']}
for code, r in sorted(js.items()):
    chk(f'state {code} outlays', r['outlays'], st.get(code) or 0)
chk('state outlays excess over total', S['state_outlays_excess_over_total'], sum(v or 0 for v in st.values()) - outl)
net_by_state = dict(con.execute("SELECT recipient_state_code, sum(federal_action_obligation) FROM t GROUP BY 1").fetchall())
for code, r in sorted(js.items()):
    chk(f'state {code} net obligations', r['net_obligations'], net_by_state[code])

# recipients
rc = dict(con.execute("""SELECT recipient_name, sum(o) FROM (SELECT recipient_name, assistance_award_unique_key,
    max(total_outlayed_amount_for_overall_award) o FROM t GROUP BY 1,2) GROUP BY 1""").fetchall())
rn = dict(con.execute("SELECT recipient_name, sum(federal_action_obligation) FROM t GROUP BY 1").fetchall())
for r in J['top_recipients']:
    chk(f"recipient {r['recipient_name'][:30]} outlays", r['outlays'], rc.get(r['recipient_name']) or 0)
    chk(f"recipient {r['recipient_name'][:30]} net obligations", r['net_obligations'], rn[r['recipient_name']])
# Names tied at the cutoff may be ordered differently, so compare the top-20 amounts, not names
top_amts = sorted(rn.values(), reverse=True)[:20]
chk('top-20 recipient net obligations identical (sorted)', int([round(r['net_obligations'], 2) for r in J['top_recipients']] == [round(x, 2) for x in top_amts]), 1)
chk('recipient outlays excess over total', S['recipient_outlays_excess_over_total'], sum(v or 0 for v in rc.values()) - outl)

# fiscal-year cohort (first positive obligation) and per-FY obligations
fy = "(CASE WHEN month(first_pos) >= 10 THEN year(first_pos) + 1 ELSE year(first_pos) END)"
coh = {r[0]: r[1:] for r in con.execute(f"SELECT {fy} f, count(*), sum(net), coalesce(sum(outl), 0) FROM a WHERE first_pos IS NOT NULL GROUP BY 1").fetchall()}
for r in J['year_comparison']:
    f = r['fiscal_year']
    chk(f'FY{f} awards first obligated', r['awards_first_obligated'], coh[f][0])
    chk(f'FY{f} cohort net obligations', r['cohort_net_obligations'], coh[f][1])
    chk(f'FY{f} cohort outlays to date', r['cohort_outlays_to_date'], coh[f][2])
    chk(f'FY{f} cohort outlay rate %', r['cohort_outlay_rate'], coh[f][2] / coh[f][1] * 100, 0.01)
    fy_act = "(CASE WHEN month(action_date) >= 10 THEN year(action_date) + 1 ELSE year(action_date) END)"
    chk(f'FY{f} net obligations by action FY', r['net_obligations'], q1(f"SELECT sum(federal_action_obligation) FROM t WHERE {fy_act} = {f}"))
    chk(f'FY{f} awards with transactions', r['awards_with_transactions'], q1(f"SELECT count(DISTINCT assistance_award_unique_key) FROM t WHERE {fy_act} = {f}"))
for r in J['outlay_by_obligation_year']:
    f = r['initial_obligation_fy']
    chk(f'cohort chart FY{f} outlays', r['total_outlays'], coh[f][2])
    chk(f'cohort chart FY{f} awards', r['total_awards'], coh[f][0])

# monthly
mo = {r[0]: r[1:] for r in con.execute("""SELECT strftime(action_date, '%Y-%m'), sum(federal_action_obligation) FILTER (WHERE federal_action_obligation > 0),
    sum(federal_action_obligation) FILTER (WHERE federal_action_obligation < 0), count(*) FROM t GROUP BY 1""").fetchall()}
mc = dict(con.execute("SELECT strftime(first_pos, '%Y-%m'), sum(outl) FROM a WHERE first_pos IS NOT NULL GROUP BY 1").fetchall())
for r in J['monthly_data']:
    m = r['month']
    chk(f'{m} obligations', r['obligations'], mo[m][0] or 0)
    chk(f'{m} deobligations', r['deobligations'], mo[m][1] or 0)
    chk(f'{m} transactions', r['transaction_count'], mo[m][2])
    chk(f'{m} outlays to date of awards first obligated', r['outlays_to_date_of_awards_first_obligated'], (mc.get(m) or 0))
chk('monthly cohort outlays sum to cohort total', sum(r['outlays_to_date_of_awards_first_obligated'] for r in J['monthly_data']), outl - S['outlays_on_awards_without_positive_obligation'])
chk('cyclical cohort outlays sum to cohort total', sum(r['cohort_outlays_to_date'] for r in J['cyclical_data']), outl - S['outlays_on_awards_without_positive_obligation'])
chk('cyclical obligations sum to net', sum(r['monthly_obligations'] for r in J['cyclical_data']), S['net_obligations'])

print(f"{'check':58} {'dashboard JSON':>20} {'independent':>20}  result")
for n, j, i, r in rows:
    print(f"{n:58} {j:>20,.2f} {i:>20,.2f}  {r}")
print(f"\n{len(rows)} checks, {len(bad)} mismatches")
sys.exit(1 if bad else 0)
