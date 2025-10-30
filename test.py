import random
from collections import defaultdict

class MockCE:
    """Mock of a Computing Element (CE) with random job stats."""
    def __init__(self, name):
        self.name = name

    def available(self):
        """Simulate available slots and current load."""
        running = random.randint(10, 200)
        waiting = random.randint(0, 50)
        max_total = 300
        return {
            "OK": True,
            "CEInfoDict": {
                "RunningJobs": running,
                "WaitingJobs": waiting,
                "MaxTotalJobs": max_total,
            },
            "Value": max_total - (running + waiting)
        }


class GreenSiteDirector:
    """Simplified SiteDirector focusing on CO₂ and efficiency-based queue ranking."""

    def __init__(self):
        self.queueDict = {}
        self.log = print  # mock logger

    def _mockExternalMetrics(self):
        """Mock external CO₂ intensity (gCO₂/kWh) and efficiency scores."""
        return {
            "CERN.ch": {"CO2": 15.4, "efficiency": 0.92},
            "RAL.uk": {"CO2": 33.1, "efficiency": 0.76},
            "IN2P3.fr": {"CO2": 24.6, "efficiency": 0.81},
            "FNAL.us": {"CO2": 40.0, "efficiency": 0.65},
        }

    def _buildMockQueueDict(self):
        """Simulate multiple queues per CE per site."""
        sites = ["CERN.ch", "RAL.uk", "IN2P3.fr", "FNAL.us"]
        for site in sites:
            for ce_num in range(1, 3):
                ce_name = f"ce{ce_num}.{site.lower().split('.')[0]}"
                ce = MockCE(ce_name)
                for q in ["short", "long"]:
                    qname = f"{site}-{ce_name}-{q}"
                    self.queueDict[qname] = {
                        "Site": site,
                        "CEName": ce_name,
                        "CE": ce,
                        "ParametersDict": {
                            "CPUTime": 3600 if q == "short" else 86400,
                            "MaxTotalJobs": 300,
                            "MaxWaitingJobs": 100,
                        },
                    }

    def _computeQueueScores(self, external_data):
        """Compute composite score using CO₂ and CE load."""
        for qname, qinfo in self.queueDict.items():
            site = qinfo["Site"]
            ce = qinfo["CE"]
            cpu_time = qinfo["ParametersDict"]["CPUTime"]

            co2 = external_data.get(site, {}).get("CO2", 100.0)
            efficiency = external_data.get(site, {}).get("efficiency", 0.5)

            # Get CE load info
            ce_stats = ce.available()["CEInfoDict"]
            running = ce_stats["RunningJobs"]
            waiting = ce_stats["WaitingJobs"]
            max_total = ce_stats["MaxTotalJobs"]
            load_factor = (running + waiting) / max_total

            # Composite score (lower = better)
            # Weighted example: carbon = 0.5, runtime = 0.3, load = 0.2
            score = (co2 * 0.5) + ((cpu_time / 3600.0) * 0.3) + (load_factor * 100 * 0.2)

            qinfo["ParametersDict"]["CompositeScore"] = round(score, 2)
            qinfo["ParametersDict"]["LoadFactor"] = round(load_factor, 2)

        # Sort queues by CompositeScore ascending
        self.queueDict = dict(
            sorted(self.queueDict.items(), key=lambda kv: kv[1]["ParametersDict"]["CompositeScore"])
        )

    def run(self):
        """Main method — builds queues, ranks them, and prints results."""
        self._buildMockQueueDict()
        ext_metrics = self._mockExternalMetrics()
        self._computeQueueScores(ext_metrics)

        print("\n=== Queue Ranking by Composite Score ===")
        for qname, qinfo in self.queueDict.items():
            site = qinfo["Site"]
            ce = qinfo["CEName"]
            cpu = qinfo["ParametersDict"]["CPUTime"]
            score = qinfo["ParametersDict"]["CompositeScore"]
            load = qinfo["ParametersDict"]["LoadFactor"]
            print(f"{qname:40} | Site: {site:10} | CE: {ce:10} | CPU: {cpu:6} | Load: {load:.2f} | Score: {score:.2f}")


# ---------------------------
# Run the mock director
# ---------------------------
if __name__ == "__main__":
    director = GreenSiteDirector()
    director.run()