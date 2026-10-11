"""Exercise the production monthly aggregation SQL without a Spark installation."""
import ast
import sqlite3
import unittest
from pathlib import Path

SOURCE = Path(__file__).resolve().parents[1] / 'databricks/src/03_gold.py'
QUERY = next(ast.literal_eval(n.value) for n in ast.parse(SOURCE.read_text(encoding='utf-8')).body
             if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'monthly_query' for t in n.targets))


def monthly(rows):
    with sqlite3.connect(':memory:') as db:
        db.row_factory = sqlite3.Row
        db.execute('CREATE TABLE supplier_schedule_month_input (report_month, scope, WERKS, LIFNR, NAME1, is_evaluable, on_time, in_full, fill_rate)')
        db.executemany('INSERT INTO supplier_schedule_month_input VALUES (?,?,?,?,?,?,?,?,?)', rows)
        return [dict(r) for r in db.execute(QUERY)]


def row(on_time, valid=1, branch='2000', scope='BRANCH', month='2026-09-01'):
    return (month, scope, branch, 'V1', 'Vendor', valid, on_time, 1 if valid else None, 1.0 if valid else None)


class ScheduleWeightTests(unittest.TestCase):
    def test_nine_on_time_plans_and_one_late_count_as_ninety_percent(self):
        r = monthly([row(1)] * 9 + [row(0)])[0]
        self.assertEqual(r['candidate_items'], 10)
        self.assertEqual(r['evaluated_items'], 10)
        self.assertAlmostEqual(r['on_time_rate'], .9)
        self.assertEqual(r['in_full_rate'], 1)
        self.assertAlmostEqual((r['on_time_rate'] + r['in_full_rate']) / 2, .95)

    def test_excluded_records_do_not_lower_rate(self):
        r = monthly([row(1), row(0, valid=0)])[0]
        self.assertEqual((r['candidate_items'], r['evaluated_items']), (2, 1))
        self.assertEqual(r['on_time_rate'], 1)

    def test_all_excluded_has_null_rates(self):
        r = monthly([row(None, valid=0)])[0]
        self.assertEqual(r['evaluated_items'], 0)
        self.assertIsNone(r['on_time_rate'])
        self.assertIsNone(r['mean_fill_rate'])

    def test_company_rate_recomputed_from_lines_not_branch_averages(self):
        branch_rows = [row(1)] * 9 + [row(0, branch='2100')]
        company_rows = [(r[0], 'ALL', None, *r[3:]) for r in branch_rows]
        result = monthly(branch_rows + company_rows + [row(0, month='2026-08-01')])
        company = next(r for r in result if r['scope'] == 'ALL')
        self.assertAlmostEqual(company['on_time_rate'], .9)
        self.assertEqual(len(result), 4)


if __name__ == '__main__':
    unittest.main()
