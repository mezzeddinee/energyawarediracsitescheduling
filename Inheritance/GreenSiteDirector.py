cat GreenSiteDirector.py
"""
GreenSiteDirector: energy-aware SiteDirector subclass.

Adds energy-aware sorting by PUE and improved queue display.
"""

import random
from DIRAC import S_OK
from DIRAC.WorkloadManagementSystem.Agent.SiteDirector import SiteDirector
from DIRAC.WorkloadManagementSystem.Utilities.QueueUtilities import getQueuesResolved
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader


class GreenSiteDirector(SiteDirector):
    """
    Inherits from SiteDirector and applies energy-aware (green) queue selection.
    """

    def __init__(self, *args, **kwargs):
        """Constructor: make sure parent initialization happens properly."""
        super().__init__(*args, **kwargs)
        self.log.always("🌱 [GreenSiteDirector] Constructor called — base class initialized.")

        # Provide safe defaults for attributes used early
        self.gridEnv = getattr(self, "gridEnv", "")
        self.workingDirectory = getattr(self, "workingDirectory", "/tmp")
        self.vo = getattr(self, "vo", "")
        self.checkPlatform = getattr(self, "checkPlatform", False)
        self.queueCECache = getattr(self, "queueCECache", {})

        # Ensure resourcesModule is available
        if not hasattr(self, "resourcesModule") or self.resourcesModule is None:
            ol = ObjectLoader()
            res = ol.loadModule("ConfigurationSystem.Client.Helpers.Resources")
            if res["OK"]:
                self.resourcesModule = res["Value"]
                self.log.always("✅ [GreenSiteDirector] Loaded resources module manually.")
            else:
                self.log.error("❌ [GreenSiteDirector] Could not load resources module:", res["Message"])

    def initialize(self):
        """Initialization with a green message."""
        self.log.always("🟢 Initializing GreenSiteDirector: energy-aware pilot scheduling activated 🌿")
        result = super().initialize()
        if not result["OK"]:
            self.log.error("Failed during parent initialization:", result["Message"])
        else:
            self.log.always("✅ [GreenSiteDirector] Parent initialization complete.")
        return result

    def _buildQueueDict(self, siteNames=None, ces=None, ceTypes=None, tags=None):
        """Override SiteDirector._buildQueueDict with energy-aware queue sorting."""
        self.log.always("♻️ [GreenSiteDirector] Building queue dictionary (no VO argument)...")

        # Ensure resources module and environment variables exist
        if not hasattr(self, "resourcesModule") or not self.resourcesModule:
            self.log.error("resourcesModule missing, loading manually...")
            ol = ObjectLoader()
            res = ol.loadModule("ConfigurationSystem.Client.Helpers.Resources")
            if res["OK"]:
                self.resourcesModule = res["Value"]
            else:
                self.log.error("Cannot load Resources module:", res["Message"])
                return res

        # Ensure safe defaults if gridEnv or workingDirectory is not yet defined
        gridEnv = getattr(self, "gridEnv", "")
        workingDir = getattr(self, "workingDirectory", "/tmp")

        try:
            # 1️⃣ Get queues
            result = self.resourcesModule.getQueues(
                community=self.vo,
                siteList=siteNames,
                ceList=ces,
                ceTypeList=ceTypes,
                tags=tags,
            )
            if not result["OK"]:
                self.log.error("Failed to get queues:", result["Message"])
                return result

            # 2️⃣ Resolve queues
            result = getQueuesResolved(
                siteDict=result["Value"],
                queueCECache=self.queueCECache,
                gridEnv=gridEnv,
                setup="unknown",
                workingDir=workingDir,
                checkPlatform=self.checkPlatform,
                instantiateCEs=True,
            )
            if not result["OK"]:
                self.log.error("Failed to resolve queues:", result["Message"])
                return result

            self.queueDict = result["Value"]

            # 3️⃣ Assign or randomize PUE if missing
            for _, qdict in self.queueDict.items():
                params = qdict.get("ParametersDict", {})
                if "PUE" not in params:
                    params["PUE"] = round(random.uniform(1.0, 2.0), 2)
                if "CI" not in params:
                    params["CI"] = round(random.uniform(100.0, 500.0), 1)

            # 4️⃣ Sort by PUE ascending (then CI ascending)
            self.queueDict = dict(
                sorted(
                    self.queueDict.items(),
                    key=lambda item: (item[1]["ParametersDict"]["PUE"], item[1]["ParametersDict"]["CI"]),
                )
            )

            self.log.always(f"✅ [GreenSiteDirector] Loaded {len(self.queueDict)} queues sorted by PUE/CI.")
            return S_OK()

        except Exception as e:
            self.log.exception("⚠️ Exception while building queue dictionary:", lException=e)
            return S_OK()

    def execute(self):
        """Pretty print sorted queues before running parent execute()."""
        self.log.always("🌿 [GreenSiteDirector] Starting execution cycle...")
        if not self.queueDict:
            self.log.warn("No queue dictionary loaded — skipping submission cycle.")
        else:
            self.log.always("📋 [GreenSiteDirector] Current queue order (by PUE / CI):")
            for i, (queue, qdict) in enumerate(self.queueDict.items(), 1):
                site = qdict.get("Site", "UnknownSite")
                ce = qdict.get("CEName", "UnknownCE")
                pue = qdict.get("ParametersDict", {}).get("PUE", "?")
                ci = qdict.get("ParametersDict", {}).get("CI", "?")
                self.log.always(f"   {i:02d}. {site:<25} | CE: {ce:<25} | PUE: {pue} | CI: {ci}")

        result = super().execute()

        if not result["OK"]:
            self.log.error("🚫 [GreenSiteDirector] Error in parent execute:", result["Message"])
        else:
            self.log.always("🌱 [GreenSiteDirector] Execution cycle completed successfully.")
        return result
