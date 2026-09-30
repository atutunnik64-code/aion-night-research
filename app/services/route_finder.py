from __future__ import annotations
from collections import defaultdict
from app.models.domain import Edge, Node


class RouteFinder:
    def __init__(self, max_hops: int = 5, min_profit_pct: float = 0.01):
        self.max_hops = max_hops
        self.min_profit_pct = min_profit_pct

    def find_cycles(self, edges: list[Edge], start_amount: float, start_assets: set[str] | None = None) -> list[dict]:
        graph: dict[str, list[Edge]] = defaultdict(list)
        nodes: dict[str, Node] = {}
        for edge in edges:
            if not edge.executable:
                continue
            graph[edge.src.key].append(edge)
            nodes[edge.src.key] = edge.src
            nodes[edge.dst.key] = edge.dst

        results: list[dict] = []
        for start_key, start_node in nodes.items():
            if start_assets and start_node.asset not in start_assets:
                continue
            self._dfs(
                graph=graph, start_key=start_key, current_key=start_key,
                initial_amount=start_amount, current_amount=start_amount,
                path=[], visited={start_key}, results=results,
            )
        unique: dict[str, dict] = {}
        for route in results:
            signature = '>'.join(route['nodes'])
            if signature not in unique or route['profit_pct'] > unique[signature]['profit_pct']:
                unique[signature] = route
        return sorted(unique.values(), key=lambda x: x['profit_pct'], reverse=True)

    def _dfs(self, graph, start_key, current_key, initial_amount, current_amount, path, visited, results):
        if len(path) >= self.max_hops:
            return
        for edge in graph.get(current_key, []):
            out = edge.apply(current_amount)
            if out <= 0:
                continue
            next_key = edge.dst.key
            next_path = path + [edge]
            if next_key == start_key and len(next_path) >= 2:
                profit_pct = (out / initial_amount - 1) * 100
                if profit_pct >= self.min_profit_pct:
                    results.append(self._format(next_path, initial_amount, out, profit_pct))
                continue
            if next_key in visited:
                continue
            self._dfs(
                graph=graph, start_key=start_key, current_key=next_key,
                initial_amount=initial_amount, current_amount=out,
                path=next_path, visited=visited | {next_key}, results=results,
            )

    def _format(self, path, start_amount, final, profit_pct):
        eta = sum(e.eta_seconds for e in path)
        nodes = [path[0].src.key] + [e.dst.key for e in path]
        return {
            'nodes': nodes,
            'start_amount': round(start_amount, 8),
            'final_amount': round(final, 8),
            'profit_amount': round(final - start_amount, 8),
            'profit_pct': round(profit_pct, 5),
            'eta_seconds': eta,
            'hops': len(path),
            'steps': [
                {
                    'from': e.src.key, 'to': e.dst.key, 'kind': e.kind,
                    'rate': e.rate, 'fee_rate': e.fee_rate,
                    'fixed_fee': e.fixed_fee, 'slippage_rate': e.slippage_rate,
                    'capacity': e.capacity, 'eta_seconds': e.eta_seconds,
                    'meta': e.meta or {},
                }
                for e in path
            ],
        }
