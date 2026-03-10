// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package yogadb

import (
	"context"
	"fmt"
	"os"

	yoga "github.com/glycerine/yogadb"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

// properties
const (
	yogaPath = "yoga.path"
	// yogaTimeout         = "yoga.timeout"
	// yogaNoGrowSync      = "yoga.no_grow_sync"
	// yogaReadOnly        = "yoga.read_only"
	// yogaMmapFlags       = "yoga.mmap_flags"
	// yogaInitialMmapSize = "yoga.initial_mmap_size"
)

type yogaCreator struct {
}

type yogaOptions struct {
	Path     string
	FileMode os.FileMode
	DBConfig *yoga.Config
}

type yogaDB struct {
	p *properties.Properties

	db *yoga.FlexDB

	r       *util.RowCodec
	bufPool *util.BufPool
}

func (c yogaCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	opts := getOptions(p)

	if p.GetBool(prop.DropData, prop.DropDataDefault) {
		os.RemoveAll(opts.Path)
	}

	db, err := yoga.OpenFlexDB(opts.Path, opts.DBConfig)
	if err != nil {
		return nil, err
	}

	return &yogaDB{
		p:       p,
		db:      db,
		r:       util.NewRowCodec(p),
		bufPool: util.NewBufPool(),
	}, nil
}

func getOptions(p *properties.Properties) yogaOptions {
	path := p.GetString(yogaPath, "/tmp/yogadb")

	opts := &yoga.Config{}

	// opts.Timeout = p.GetDuration(yogaTimeout, 0)
	// opts.NoGrowSync = p.GetBool(yogaNoGrowSync, false)
	// opts.ReadOnly = p.GetBool(yogaReadOnly, false)
	// opts.MmapFlags = p.GetInt(yogaMmapFlags, 0)
	// opts.InitialMmapSize = p.GetInt(yogaInitialMmapSize, 0)

	return yogaOptions{
		Path:     path,
		FileMode: 0600,
		DBConfig: opts,
	}
}

func (db *yogaDB) Close() error {
	db.db.Close()
	return nil
}

func (db *yogaDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *yogaDB) CleanupThread(_ context.Context) {
}

func (db *yogaDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	var m map[string][]byte
	err := db.db.View(func(tx *yoga.ReadOnlyTx) error {
		//bucket := tx.Bucket([]byte(table))
		//if bucket == nil {
		//	return fmt.Errorf("table not found: %s", table)
		//}

		row, ok := db.db.Get(table + "/" + key)
		if !ok {
			return fmt.Errorf("key not found: %s.%s", table, key)
		}

		var err error
		m, err = db.r.Decode(row, fields)
		return err
	})
	return m, err
}

func (db *yogaDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	res := make([]map[string][]byte, count)
	err := db.db.View(func(ro *yoga.ReadOnlyTx) error {
		// bucket := tx.Bucket([]byte(table))
		// if bucket == nil {
		// 	return fmt.Errorf("table not found: %s", table)
		// }

		it := ro.NewIter()
		//cursor := bucket.Cursor()
		//key, value := cursor.Seek([]byte(table + "/" + startKey))
		it.Seek(table + "/" + startKey)

		for i := 0; it.Valid() && i < count; i++ {
			_, value, found, err := it.GetAnySize()
			if err != nil {
				return err
			}
			if found {
				m, err := db.r.Decode(value, fields)
				if err != nil {
					return err
				}

				res[i] = m
			}
			it.Next()
		}

		return nil
	})
	return res, err
}

func (db *yogaDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	err := db.db.Update(func(tx *yoga.WriteTx) error {
		// bucket := tx.Bucket([]byte(table))
		// if bucket == nil {
		// 	return fmt.Errorf("table not found: %s", table)
		// }

		value, found := tx.Get(table + "/" + key)
		if !found {
			return fmt.Errorf("key not found: %s.%s", table, key)
		}

		data, err := db.r.Decode(value, nil)
		if err != nil {
			return err
		}

		for field, value := range values {
			data[field] = value
		}

		buf := db.bufPool.Get()
		defer func() {
			db.bufPool.Put(buf)
		}()

		buf, err = db.r.Encode(buf, data)
		if err != nil {
			return err
		}

		return tx.Put(key, buf)
	})
	return err
}

func (db *yogaDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	err := db.db.Update(func(tx *yoga.WriteTx) error {
		// bucket, err := tx.CreateBucketIfNotExists([]byte(table))
		// if err != nil {
		// 	return err
		// }

		buf := db.bufPool.Get()
		defer func() {
			db.bufPool.Put(buf)
		}()

		var err error
		buf, err = db.r.Encode(buf, values)
		if err != nil {
			return err
		}

		return tx.Put(table+"/"+key, buf)
	})
	return err
}

func (db *yogaDB) Delete(ctx context.Context, table string, key string) error {
	err := db.db.Update(func(tx *yoga.WriteTx) error {
		// bucket := tx.Bucket([]byte(table))
		// if bucket == nil {
		// 	return nil
		// }

		err := db.db.Delete(key)
		if err != nil {
			return err
		}

		// if bucket.Stats().KeyN == 0 {
		// 	_ = tx.DeleteBucket([]byte(table))
		// }
		return nil
	})
	return err
}

func init() {
	ycsb.RegisterDBCreator("yogadb", yogaCreator{})
}
