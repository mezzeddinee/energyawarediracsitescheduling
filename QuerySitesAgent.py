"""iteInfoAgent (DIRAC 8.x Compatible)
------------------------------------

A lightweight WorkloadManagementSystem agent that lists all available
Sites, CEs, and Queues for a given VO, including queue parameters and
live CE status (Running/Waiting/Max jobs).

Author: Your Name
Tested on DIRAC v8.0.75
"""

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule

# ✅ Correct imports for DIRAC 8.0.75
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getQueues
from DIRAC.WorkloadManagementSystem.Utilities.QueueUtilities import getQueuesResolved
from DIRAC.ConfigurationSystem.Client.Helpers.CSGlobals import getVO


class SiteInfoAgent(AgentModule):
    """Agent that prints site, CE, and queue info."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.vo = None
        self.queueDict = {}
        self.queueCECache = {}




    # ------------------------------------------------------------------
    def initialize(self):
        """Initialize the agent — set VO and check configuration."""
        self.vo = self.am_getOption("VO", getVO() or "biomed")

        if not self.vo:
            return S_ERROR("No VO specified or found in CSGlobals")

        self.log.always(f"[SiteInfoAgent] Initialized for VO='{self.vo}'")
        return S_OK()

        # ------------------------------------------------------------------

    def execute(self):
        """Main agent cycle — retrieves and prints site/queue info."""
        self.log.always("=== [SiteInfoAgent] Cycle started ===")

        # 1️⃣ Get queues from Configuration Service
        result = getQueues(community=self.vo)
        if not result["OK"]:
            self.log.error("Failed to get queues from CS", result["Message"])
            return result

        siteDict = result["Value"]
        if not siteDict:
            self.log.warn("No sites or queues found for this VO")
            return S_OK()

        # 2️⃣ Resolve queue information into CE objects
        self.queueCECache = {}
        resolved = getQueuesResolved(siteDict, self.queueCECache, instantiateCEs=True)
        if not resolved["OK"]:
            self.log.error("Failed to resolve CE objects", resolved["Message"])
            return resolved

        self.queueDict = resolved["Value"]

        # 3️⃣ Loop over all queues and print details
        for queueName, qdict in self.queueDict.items():
            site = qdict.get("Site", "UnknownSite")
            ceName = qdict.get("CEName", "UnknownCE")
            ceType = qdict.get("CEType", "UnknownType")
            queue = qdict.get("QueueName", "UnknownQueue")
            params = qdict.get("ParametersDict", {})

            cpu = params.get("CPUTime", "N/A")
            maxJobs = params.get("MaxTotalJobs", "N/A")
            waitingLimit = params.get("MaxWaitingJobs", "N/A")

            self.log.always(f"\n🌐 Site: {site}")
            self.log.always(f"   CE: {ceName} ({ceType})")
            self.log.always(f"   Queue: {queue}")
            self.log.always(f"     CPUTime={cpu}, MaxJobs={maxJobs}, MaxWaiting={waitingLimit}")

            # 4️⃣ Try to fetch live CE info
            ce = qdict.get("CE")
            if ce:
                try:
                    result = ce.available()
                    if result["OK"]:
                        ceInfo = result["CEInfoDict"]
                        self.log.always(
                            f"     LiveStatus → Running={ceInfo['RunningJobs']}, "
                            f"Waiting={ceInfo['WaitingJobs']}, Max={ceInfo['MaxTotalJobs']}"
                        )
                    else:
                        self.log.warn(f"     CE {ceName}: {result['Message']}")
                except Exception as e:
                    self.log.warn(f"     CE {ceName} could not be contacted: {e}")
            else:
                self.log.warn(f"     No CE object found for queue {queue}")

        self.log.always("\n=== [SiteInfoAgent] Cycle complete ===")
        return S_OK()


