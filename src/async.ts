import { IKeyValueStoreAsync } from "@konceiver/kv";

import { DatabaseOptions } from "./interfaces";
import { StoreSync } from "./sync";

export class StoreAsync<K, T> implements IKeyValueStoreAsync<K, T> {
	private constructor(private readonly store: StoreSync<K, T>) {}

	public static async new<K, T>(
		opts: DatabaseOptions
	): Promise<StoreAsync<K, T>> {
		return new StoreAsync<K, T>(StoreSync.new<K, T>(opts));
	}

	public async all(): Promise<[K, T][]> {
		return this.store.all();
	}

	public async keys(): Promise<K[]> {
		return this.store.keys();
	}

	public async values(): Promise<T[]> {
		return this.store.values();
	}

	public async get(key: K): Promise<T | undefined> {
		return this.store.get(key);
	}

	public async getMany(keys: K[]): Promise<(T | undefined)[]> {
		return this.store.getMany(keys);
	}

	public async pull(key: K): Promise<T | undefined> {
		return this.store.pull(key);
	}

	public async pullMany(keys: K[]): Promise<(T | undefined)[]> {
		return this.store.pullMany(keys);
	}

	public async put(key: K, value: T): Promise<boolean> {
		return this.store.put(key, value);
	}

	public async putMany(values: [K, T][]): Promise<boolean[]> {
		return this.store.putMany(values);
	}

	public async has(key: K): Promise<boolean> {
		return this.store.has(key);
	}

	public async hasMany(keys: K[]): Promise<boolean[]> {
		return this.store.hasMany(keys);
	}

	public async missing(key: K): Promise<boolean> {
		return this.store.missing(key);
	}

	public async missingMany(keys: K[]): Promise<boolean[]> {
		return this.store.missingMany(keys);
	}

	public async forget(key: K): Promise<boolean> {
		return this.store.forget(key);
	}

	public async forgetMany(keys: K[]): Promise<boolean[]> {
		return this.store.forgetMany(keys);
	}

	public async flush(): Promise<boolean> {
		return this.store.flush();
	}

	public async count(): Promise<number> {
		return this.store.count();
	}

	public async isEmpty(): Promise<boolean> {
		return this.store.isEmpty();
	}

	public async isNotEmpty(): Promise<boolean> {
		return this.store.isNotEmpty();
	}
}
