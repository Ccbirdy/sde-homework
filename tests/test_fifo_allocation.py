"""Run the production interval SQL against SQLite; Spark execution is checked separately.

SQLite supports the SQL window/join subset used here. Decimal typing is not equivalent
to Spark, so fractional assertions use tolerance; these tests do not validate Spark types.
"""
import ast
import random
import sqlite3
import unittest
from pathlib import Path


SOURCE = Path(__file__).resolve().parents[1] / "databricks/src/03_gold.py"
QUERY = next(ast.literal_eval(node.value) for node in ast.parse(SOURCE.read_text(encoding="utf-8")).body
             if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "allocation_query" for t in node.targets))


def allocate(plans, deliveries):
    with sqlite3.connect(":memory:") as db:
        db.row_factory = sqlite3.Row
        db.create_function("LEAST", 2, min)
        db.create_function("GREATEST", 2, max)
        db.execute("CREATE TABLE fifo_schedule_input (MANDT, EBELN, EBELP, ETENR, EINDT, schedule_quantity, WERKS, LIFNR, MATNR, MEINS)")
        db.execute("CREATE TABLE fifo_delivery_input (MANDT, EBELN, EBELP, VBELN, POSNR, WADAT_IST, LFIMG)")
        db.executemany("INSERT INTO fifo_schedule_input VALUES (?,?,?,?,?,?,?,?,?,?)",
                       [(client, order, item, plan, day, qty, '2000', 'V1', 'M1', 'ST')
                        for client, order, item, plan, day, qty in plans])
        db.executemany("INSERT INTO fifo_delivery_input VALUES (?,?,?,?,?,?,?)", deliveries)
        return [dict(r) for r in db.execute(QUERY)]


def plan(pid, day, qty, order="P1", client="300"):
    return client, order, "00010", pid, day, qty


def delivery(did, day, qty, order="P1", client="300"):
    return client, order, "00010", did, "000010", day, qty


class FifoTests(unittest.TestCase):
    def test_split_and_combine_with_late_first_plan(self):
        rows = allocate([plan('1', '2026-09-10', 40), plan('2', '2026-09-20', 60)],
                        [delivery('D1', '2026-09-12', 50), delivery('D2', '2026-09-19', 50)])
        self.assertEqual({(r['ETENR'], r['VBELN']): r['allocated_quantity'] for r in rows},
                         {('1', 'D1'): 40, ('2', 'D1'): 10, ('2', 'D2'): 50})
        on_time = {pid: sum(r['allocated_quantity'] for r in rows
                           if r['ETENR'] == pid and r['WADAT_IST'] <= r['EINDT']) for pid in ('1', '2')}
        self.assertEqual(on_time, {'1': 0, '2': 60})

    def test_partial_delivery(self):
        rows = allocate([plan('1', '2026-09-10', 40), plan('2', '2026-09-20', 60)],
                        [delivery('D1', '2026-09-09', 25)])
        self.assertEqual([(r['ETENR'], r['allocated_quantity']) for r in rows], [('1', 25)])

    def test_excess_does_not_overfill(self):
        rows = allocate([plan('1', '2026-09-10', 40)], [delivery('D1', '2026-09-09', 100)])
        self.assertEqual(sum(r['allocated_quantity'] for r in rows), 40)

    def test_cross_month_lateness(self):
        rows = allocate([plan('1', '2026-08-31', 40), plan('2', '2026-09-10', 60)],
                        [delivery('D1', '2026-09-01', 100)])
        august_received = sum(r['allocated_quantity'] for r in rows if r['ETENR'] == '1' and r['WADAT_IST'] <= '2026-08-31')
        self.assertEqual(august_received, 0)
        self.assertEqual(sum(r['allocated_quantity'] for r in rows if r['ETENR'] == '1'), 40)

    def test_equal_dates_are_deterministic(self):
        plans = [plan('2', '2026-09-10', 10), plan('1', '2026-09-10', 10)]
        deliveries = [delivery('D2', '2026-09-10', 10), delivery('D1', '2026-09-10', 10)]
        rows = allocate(plans, deliveries)
        self.assertEqual({(r['ETENR'], r['VBELN']) for r in rows}, {('1', 'D1'), ('2', 'D2')})
        self.assertEqual(rows, allocate(plans[::-1], deliveries[::-1]))

    def test_no_delivery_or_no_schedule(self):
        self.assertEqual(allocate([plan('1', '2026-09-10', 40)], []), [])
        self.assertEqual(allocate([], [delivery('D1', '2026-09-09', 40)]), [])

    def test_partition_isolation(self):
        rows = allocate([plan('1', '2026-09-10', 40)],
                        [delivery('D1', '2026-09-09', 40, order='P2'),
                         delivery('D2', '2026-09-09', 40, client='301')])
        self.assertEqual(rows, [])

    def test_fractional_and_random_quantity_conservation(self):
        rng = random.Random(42)
        for _ in range(50):
            plans = [plan(str(i), f'2026-09-{i+1:02}', rng.randint(1, 1000000) / 1000000) for i in range(5)]
            deliveries = [delivery(str(i), f'2026-09-{i+1:02}', rng.randint(1, 1000000) / 1000000) for i in range(6)]
            rows = allocate(plans, deliveries)
            self.assertAlmostEqual(sum(r['allocated_quantity'] for r in rows),
                                   min(sum(p[-1] for p in plans), sum(d[-1] for d in deliveries)))
            for p in plans:
                self.assertLessEqual(sum(r['allocated_quantity'] for r in rows if r['ETENR'] == p[3]), p[-1] + 1e-9)
            for d in deliveries:
                self.assertLessEqual(sum(r['allocated_quantity'] for r in rows if r['VBELN'] == d[3]), d[-1] + 1e-9)


if __name__ == '__main__':
    unittest.main()
