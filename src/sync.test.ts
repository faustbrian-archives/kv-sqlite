import { complianceTestsSync } from "@konceiver/kv-test-suite";
import tempy from "tempy";

import { StoreSync } from "../src/sync";

complianceTestsSync(
	() =>
		StoreSync.new<string, number>({
			connection: tempy.file(),
			type: "INTEGER",
		}),
	{
		"1": 1,
		"2": 2,
		"3": 3,
		"4": 4,
		"5": 5,
	}
);
