package storage

import(
	// "time"
	"testing"
	"math/big"
	"encoding/hex"
)

const (
	TEST_BLOB_PATH = "../../data/tests/test.bl"
)

func testProjectionConversion(t *testing.T, b *Blob, i, cur int64) {
	index := big.NewInt(i)
	cursor := int64(cur)
	if b.SlotOf(index) != cursor {
		t.Errorf("Cursor position wrong: %d vs %d", index, cursor)
	}
}

func TestBlobAutoCapacity(t *testing.T) {
	//
	// use 1 byte keys space, cap is eq to -1, which means
	// allocation of 2 ** (2 ** 3 * 1) rows 2 bytes each long
	//
	b := &Blob{
		Path: TEST_BLOB_PATH,
		Capacity: 0,
		KeySize: 1,
		ValueSize: 2,
	}
	maxExpected := int64(256)
	capSizeExpected := int64(256 * 2)

	if err := b.Init(); err != nil {
		t.Errorf(err.Error())
	}
	if b.file == nil {
		t.Errorf("File should not be nil")
	}

	if b.Capacity != 0 {
		t.Errorf("Wrong Capacity: %d vs %d", b.Capacity, 0)
	}

	capSize := b.CapacitySize()
	if capSize != capSizeExpected {
		t.Errorf("Wrong Capacity: %d vs %d", capSize, capSizeExpected)
	}

	recordsCount := b.RecordsCount()
	if recordsCount != maxExpected {
		t.Errorf("Wrong Capacity: %d vs %d", recordsCount, maxExpected)
	}

	if err := b.Destroy(); err != nil {
		t.Errorf(err.Error())
	}
}

func TestBlobDeclaredCapacity(t *testing.T) {
	//
	// use 1 byte keys space, cap is eq to -1, which means
	// allocation of 2 ** (2 ** 3 * 1) rows 2 bytes each long
	//
	blob := &Blob{
		Path: TEST_BLOB_PATH,
		Capacity: 10, // 2 ** 10
		KeySize: 4,
		ValueSize: 3,
	}
	maxExpected := int64(1024)
	capSizeExpected := int64(1024 * (4 + 3))

	if err := blob.Init(); err != nil {
		t.Errorf(err.Error())
	}
	if blob.file == nil {
		t.Errorf("File should not be nil")
	}

	if blob.Capacity != 10 {
		t.Errorf("Wrong Capacity: %d vs %d", blob.Capacity, 10)
	}

	capSize := blob.CapacitySize()
	if capSize != capSizeExpected {
		t.Errorf("Wrong Capacity: %d vs %d", capSize, capSizeExpected)
	}

	recordsCount := blob.RecordsCount()
	if recordsCount != maxExpected {
		t.Errorf("Wrong Capacity: %d vs %d", recordsCount, maxExpected)
	}

	testProjectionConversion(t, blob, 0, 0)
	testProjectionConversion(t, blob, 4194304, 1)
	testProjectionConversion(t, blob, 8388608, 2)
	testProjectionConversion(t, blob, 16777216, 4)
	testProjectionConversion(t, blob, 33554432, 8)
	testProjectionConversion(t, blob, 67108864, 16)
	testProjectionConversion(t, blob, 134217728, 32)
	testProjectionConversion(t, blob, 268435456, 64)
	testProjectionConversion(t, blob, 536870912, 128)
	testProjectionConversion(t, blob, 1073741824, 256)
	testProjectionConversion(t, blob, 2147483648, 512)
	testProjectionConversion(t, blob, 2151677952, 513)
	testProjectionConversion(t, blob, 4294967296, 1024)

	data, address, iters, err := blob.Get(big.NewInt(0))
	if err != nil {
		t.Errorf(err.Error())
	}
	if address != 0 {
		t.Errorf("Expected to get the item with [0] address, but got at: %d", address)
	}
	if iters != 1 {
		t.Errorf("Expected to get in 1 iteration, but got: %d", iters)
	}

	data_s := hex.EncodeToString(data)
	data_s_expected := "000000"
	if data_s != data_s_expected {
		t.Errorf("Data mismatch: %s vs %s", data_s, data_s_expected)
	}


	//
	// set record
	//
	key1 := big.NewInt(33554433)
	recordValue1, _ := hex.DecodeString("101ac1")
	// start := time.Now()
	address, iters, err = blob.Set(key1, &recordValue1)
	// elapsed := time.Since(start)
	// t.Errorf("%s", elapsed)
	if err != nil {
		t.Errorf(err.Error())
	}


	key := big.NewInt(33554432)
	recordValue, _ := hex.DecodeString("aabbcc")
	address, iters, err = blob.Set(key, &recordValue)
	if err != nil {
		t.Errorf(err.Error())
	}
	if address != 9 {
		t.Errorf("Expected to get the item with [8] address, but got at: %d", address)
	}
	if iters != 2 {
		t.Errorf("Expected to get in 1 iteration, but got: %d", iters)
	}

	//
	// get record
	//
	key = big.NewInt(33554432)
	data, address, iters, err = blob.Get(key)

	if err != nil {
		t.Errorf(err.Error())
	}
	if address != 9 {
		t.Errorf("Expected to get the item with [8] address, but got at: %d", address)
	}
	if iters != 2 {
		t.Errorf("Expected to get in 1 iteration, but got: %d", iters)
	}
	if hex.EncodeToString(data) != "aabbcc" {
		t.Errorf("Expected different data: %s vs %s", hex.EncodeToString(data), "aabbcc")
	}


	if err := blob.Destroy(); err != nil {
		t.Errorf(err.Error())
	}
}

func BenchmarkCapacityIndexCalc(b *testing.B) {
	b.ReportAllocs()
	blob := &Blob{
		Path: TEST_BLOB_PATH,
		Capacity: 12, // 2 ** 10
		KeySize: 8,
		ValueSize: 4,
	}
	blob.Init()
	index := int64(0)
	step := int64(1198372)

	for i := 0; i < b.N; i++ {
		for ind := 1; ind <= 1024; ind++ {
    		index += step
    		blob.SlotOf(big.NewInt(index))
		}
	}
	blob.Destroy()
}
