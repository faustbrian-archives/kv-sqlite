import { complianceTestsAsync } from "@keeveestore/test-suite";
import tempy from "tempy";
import { StoreAsync } from "../src/async";

complianceTestsAsync(() => StoreAsync.new<string, number>({ connection: tempy.file(), type: "INTEGER" }), {
	1: 1,
	2: 2,
	3: 3,
	4: 4,
	5: 5,
});
