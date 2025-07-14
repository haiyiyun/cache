package cache

import (
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

type TestStruct struct {
	Num      int
	Children []*TestStruct
}

func TestCache(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	var a int
	found, err := tc.Get("a", &a)
	if found || err != nil {
		t.Error("Getting A found value that shouldn't exist:", a)
	}

	var b string
	found, err = tc.Get("b", &b)
	if found || err != nil {
		t.Error("Getting B found value that shouldn't exist:", b)
	}

	var c float64
	found, err = tc.Get("c", &c)
	if found || err != nil {
		t.Error("Getting C found value that shouldn't exist:", c)
	}

	tc.Set("a", 1, DefaultExpiration)
	tc.Set("b", "b", DefaultExpiration)
	tc.Set("c", 3.5, DefaultExpiration)

	found, err = tc.Get("a", &a)
	if !found {
		t.Error("a was not found while getting a")
	}
	if err != nil {
		t.Error(err)
	} else if a+2 != 3 {
		t.Error("a2 (which should be 1) plus 2 does not equal 3; value:", a)
	}

	found, err = tc.Get("b", &b)
	if !found {
		t.Error("b was not found while getting b")
	}
	if err != nil {
		t.Error(err)
	} else if b+"B" != "bB" {
		t.Error("b2 (which should be b) plus B does not equal bB; value:", b)
	}

	found, err = tc.Get("c", &c)
	if !found {
		t.Error("c was not found while getting c")
	}
	if err != nil {
		t.Error(err)
	} else if c+1.2 != 4.7 {
		t.Error("c2 (which should be 3.5) plus 1.2 does not equal 4.7; value:", c)
	}
}

func TestCacheTimes(t *testing.T) {
	var found bool
	a := 1
	b := 2
	c := 3
	d := 4
	tc := NewMemoryCache(50*time.Millisecond, 1*time.Millisecond)
	tc.Set("a", a, DefaultExpiration)
	tc.Set("b", b, NoExpiration)
	tc.Set("c", c, 20*time.Millisecond)
	tc.Set("d", d, 70*time.Millisecond)

	<-time.After(25 * time.Millisecond)
	found, _ = tc.Get("c", &c)
	if found {
		t.Error("Found c when it should have been automatically deleted")
	}

	<-time.After(30 * time.Millisecond)
	found, _ = tc.Get("a", &a)
	if found {
		t.Error("Found a when it should have been automatically deleted")
	}

	found, _ = tc.Get("b", &b)
	if !found {
		t.Error("Did not find b even though it was set to never expire")
	}

	found, _ = tc.Get("d", &d)
	if !found {
		t.Error("Did not find d even though it was set to expire later than the default")
	}

	<-time.After(20 * time.Millisecond)
	found, _ = tc.Get("d", &d)
	if found {
		t.Error("Found d when it should have been automatically deleted (later than the default)")
	}
}

func TestNewFrom(t *testing.T) {
	m := map[string]Item{
		"a": Item{
			Object:     1,
			Expiration: 0,
		},
		"b": Item{
			Object:     2,
			Expiration: 0,
		},
	}
	tc := NewMemoryCacheFrom(DefaultExpiration, 0, m)
	var a int
	found, _ := tc.Get("a", &a)
	if !found {
		t.Fatal("Did not find a")
	}
	if a != 1 {
		t.Fatal("a is not 1")
	}

	var b int
	found, _ = tc.Get("b", &b)
	if !found {
		t.Fatal("Did not find b")
	}
	if b != 2 {
		t.Fatal("b is not 2")
	}
}

func TestStorePointerToStruct(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	tc.Set("foo", &TestStruct{Num: 1}, DefaultExpiration)
	var foo *TestStruct
	found, _ := tc.Get("foo", &foo)
	if !found {
		t.Fatal("*TestStruct was not found for foo")
	}

	foo.Num++

	var bar *TestStruct
	found, _ = tc.Get("foo", &bar)
	if !found {
		t.Fatal("*TestStruct was not found for foo (second time)")
	}

	if bar.Num != 2 {
		t.Fatal("TestStruct.Num is not 2")
	}
}

func TestIncrementWithInt(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	var x int
	tc.Set("tint", 1, DefaultExpiration)
	err := tc.Increment("tint", 2)
	if err != nil {
		t.Error("Error incrementing:", err)
	}
	found, _ := tc.Get("tint", &x)
	if !found {
		t.Error("tint was not found")
	}
	if x != 3 {
		t.Error("tint is not 3:", x)
	}
}

func TestIncrementWithInt8(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	var x int8
	tc.Set("tint8", int8(1), DefaultExpiration)
	err := tc.Increment("tint8", 2)
	if err != nil {
		t.Error("Error incrementing:", err)
	}
	found, _ := tc.Get("tint8", &x)
	if !found {
		t.Error("tint8 was not found")
	}
	if x != 3 {
		t.Error("tint8 is not 3:", x)
	}
}

func TestIncrementWithInt16(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	var x int16
	tc.Set("tint16", int16(1), DefaultExpiration)
	err := tc.Increment("tint16", 2)
	if err != nil {
		t.Error("Error incrementing:", err)
	}
	found, _ := tc.Get("tint16", &x)
	if !found {
		t.Error("tint16 was not found")
	}
	if x != 3 {
		t.Error("tint16 is not 3:", x)
	}
}

func TestIncrementWithInt32(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	var x int32
	tc.Set("tint32", int32(1), DefaultExpiration)
	err := tc.Increment("tint32", 2)
	if err != nil {
		t.Error("Error incrementing:", err)
	}
	found, _ := tc.Get("tint32", &x)
	if !found {
		t.Error("tint32 was not found")
	}
	if x != 3 {
		t.Error("tint32 is not 3:", x)
	}
}

func TestIncrementWithInt64(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	var x int64
	tc.Set("tint64", int64(1), DefaultExpiration)
	err := tc.Increment("tint64", 2)
	if err != nil {
		t.Error("Error incrementing:", err)
	}
	found, _ := tc.Get("tint64", &x)
	if !found {
		t.Error("tint64 was not found")
	}
	if x != 3 {
		t.Error("tint64 is not 3:", x)
	}
}

func TestIncrementWithUint(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	var x uint
	tc.Set("tuint", uint(1), DefaultExpiration)
	err := tc.Increment("tuint", 2)
	if err != nil {
		t.Error("Error incrementing:", err)
	}
	found, _ := tc.Get("tuint", &x)
	if !found {
		t.Error("tuint was not found")
	}
	if x != 3 {
		t.Error("tuint is not 3:", x)
	}
}

func TestIncrementWithUintptr(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	var x uintptr
	tc.Set("tuintptr", uintptr(1), DefaultExpiration)
	err := tc.Increment("tuintptr", 2)
	if err != nil {
		t.Error("Error incrementing:", err)
	}

	found, _ := tc.Get("tuintptr", &x)
	if !found {
		t.Error("tuintptr was not found")
	}
	if x != 3 {
		t.Error("tuintptr is not 3:", x)
	}
}

func TestIncrementWithUint8(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	var x uint8
	tc.Set("tuint8", uint8(1), DefaultExpiration)
	err := tc.Increment("tuint8", 2)
	if err != nil {
		t.Error("Error incrementing:", err)
	}
	found, _ := tc.Get("tuint8", &x)
	if !found {
		t.Error("tuint8 was not found")
	}
	if x != 3 {
		t.Error("tuint8 is not 3:", x)
	}
}

func TestIncrementWithUint16(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	var x uint16
	tc.Set("tuint16", uint16(1), DefaultExpiration)
	err := tc.Increment("tuint16", 2)
	if err != nil {
		t.Error("Error incrementing:", err)
	}

	found, _ := tc.Get("tuint16", &x)
	if !found {
		t.Error("tuint16 was not found")
	}
	if x != 3 {
		t.Error("tuint16 is not 3:", x)
	}
}

func TestIncrementWithUint32(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	var x uint32
	tc.Set("tuint32", uint32(1), DefaultExpiration)
	err := tc.Increment("tuint32", 2)
	if err != nil {
		t.Error("Error incrementing:", err)
	}
	found, _ := tc.Get("tuint32", &x)
	if !found {
		t.Error("tuint32 was not found")
	}
	if x != 3 {
		t.Error("tuint32 is not 3:", x)
	}
}

func TestIncrementWithUint64(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	var x uint64
	tc.Set("tuint64", uint64(1), DefaultExpiration)
	err := tc.Increment("tuint64", 2)
	if err != nil {
		t.Error("Error incrementing:", err)
	}

	found, _ := tc.Get("tuint64", &x)
	if !found {
		t.Error("tuint64 was not found")
	}
	if x != 3 {
		t.Error("tuint64 is not 3:", x)
	}
}

func TestIncrementWithFloat32(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	var x float32
	tc.Set("float32", float32(1.5), DefaultExpiration)
	err := tc.Increment("float32", 2)
	if err != nil {
		t.Error("Error incrementing:", err)
	}
	found, _ := tc.Get("float32", &x)
	if !found {
		t.Error("float32 was not found")
	}
	if x != 3.5 {
		t.Error("float32 is not 3.5:", x)
	}
}

func TestIncrementWithFloat64(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	var x float64
	tc.Set("float64", float64(1.5), DefaultExpiration)
	err := tc.Increment("float64", 2)
	if err != nil {
		t.Error("Error incrementing:", err)
	}
	found, _ := tc.Get("float64", &x)
	if !found {
		t.Error("float64 was not found")
	}
	if x != 3.5 {
		t.Error("float64 is not 3.5:", x)
	}
}

func TestIncrementFloatWithFloat32(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	var x float32
	tc.Set("float32", float32(1.5), DefaultExpiration)
	err := tc.IncrementFloat("float32", 2)
	if err != nil {
		t.Error("Error incrementfloating:", err)
	}
	found, _ := tc.Get("float32", &x)
	if !found {
		t.Error("float32 was not found")
	}
	if x != 3.5 {
		t.Error("float32 is not 3.5:", x)
	}
}

func TestIncrementFloatWithFloat64(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	var x float64
	tc.Set("float64", float64(1.5), DefaultExpiration)
	err := tc.IncrementFloat("float64", 2)
	if err != nil {
		t.Error("Error incrementfloating:", err)
	}
	found, _ := tc.Get("float64", &x)
	if !found {
		t.Error("float64 was not found")
	}
	if x != 3.5 {
		t.Error("float64 is not 3.5:", x)
	}
}

func TestDecrementWithInt(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	var x int
	tc.Set("int", int(5), DefaultExpiration)
	err := tc.Decrement("int", 2)
	if err != nil {
		t.Error("Error decrementing:", err)
	}
	found, _ := tc.Get("int", &x)
	if !found {
		t.Error("int was not found")
	}
	if x != 3 {
		t.Error("int is not 3:", x)
	}
}

func TestDecrementWithInt8(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	var x int8
	tc.Set("int8", int8(5), DefaultExpiration)
	err := tc.Decrement("int8", 2)
	if err != nil {
		t.Error("Error decrementing:", err)
	}
	found, _ := tc.Get("int8", &x)
	if !found {
		t.Error("int8 was not found")
	}
	if x != 3 {
		t.Error("int8 is not 3:", x)
	}
}

func TestDecrementWithInt16(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	var x int16
	tc.Set("int16", int16(5), DefaultExpiration)
	err := tc.Decrement("int16", 2)
	if err != nil {
		t.Error("Error decrementing:", err)
	}
	found, _ := tc.Get("int16", &x)
	if !found {
		t.Error("int16 was not found")
	}
	if x != 3 {
		t.Error("int16 is not 3:", x)
	}
}

func TestDecrementWithInt32(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	var x int32
	tc.Set("int32", int32(5), DefaultExpiration)
	err := tc.Decrement("int32", 2)
	if err != nil {
		t.Error("Error decrementing:", err)
	}
	found, _ := tc.Get("int32", &x)
	if !found {
		t.Error("int32 was not found")
	}
	if x != 3 {
		t.Error("int32 is not 3:", x)
	}
}

func TestDecrementWithInt64(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	var x int64
	tc.Set("int64", int64(5), DefaultExpiration)
	err := tc.Decrement("int64", 2)
	if err != nil {
		t.Error("Error decrementing:", err)
	}
	found, _ := tc.Get("int64", &x)
	if !found {
		t.Error("int64 was not found")
	}
	if x != 3 {
		t.Error("int64 is not 3:", x)
	}
}

func TestDecrementWithUint(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	var x uint
	tc.Set("uint", uint(5), DefaultExpiration)
	err := tc.Decrement("uint", 2)
	if err != nil {
		t.Error("Error decrementing:", err)
	}
	found, _ := tc.Get("uint", &x)
	if !found {
		t.Error("uint was not found")
	}
	if x != 3 {
		t.Error("uint is not 3:", x)
	}
}

func TestDecrementWithUintptr(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	var x uintptr
	tc.Set("uintptr", uintptr(5), DefaultExpiration)
	err := tc.Decrement("uintptr", 2)
	if err != nil {
		t.Error("Error decrementing:", err)
	}
	found, _ := tc.Get("uintptr", &x)
	if !found {
		t.Error("uintptr was not found")
	}
	if x != 3 {
		t.Error("uintptr is not 3:", x)
	}
}

func TestDecrementWithUint8(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	var x uint8
	tc.Set("uint8", uint8(5), DefaultExpiration)
	err := tc.Decrement("uint8", 2)
	if err != nil {
		t.Error("Error decrementing:", err)
	}
	found, _ := tc.Get("uint8", &x)
	if !found {
		t.Error("uint8 was not found")
	}
	if x != 3 {
		t.Error("uint8 is not 3:", x)
	}
}

func TestDecrementWithUint16(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	var x uint16
	tc.Set("uint16", uint16(5), DefaultExpiration)
	err := tc.Decrement("uint16", 2)
	if err != nil {
		t.Error("Error decrementing:", err)
	}
	found, _ := tc.Get("uint16", &x)
	if !found {
		t.Error("uint16 was not found")
	}
	if x != 3 {
		t.Error("uint16 is not 3:", x)
	}
}

func TestDecrementWithUint32(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	var x uint32
	tc.Set("uint32", uint32(5), DefaultExpiration)
	err := tc.Decrement("uint32", 2)
	if err != nil {
		t.Error("Error decrementing:", err)
	}
	found, _ := tc.Get("uint32", &x)
	if !found {
		t.Error("uint32 was not found")
	}
	if x != 3 {
		t.Error("uint32 is not 3:", x)
	}
}

func TestDecrementWithUint64(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	var x uint64
	tc.Set("uint64", uint64(5), DefaultExpiration)
	err := tc.Decrement("uint64", 2)
	if err != nil {
		t.Error("Error decrementing:", err)
	}
	found, _ := tc.Get("uint64", &x)
	if !found {
		t.Error("uint64 was not found")
	}
	if x != 3 {
		t.Error("uint64 is not 3:", x)
	}
}

func TestDecrementWithFloat32(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	var x float32
	tc.Set("float32", float32(5.5), DefaultExpiration)
	err := tc.Decrement("float32", 2)
	if err != nil {
		t.Error("Error decrementing:", err)
	}
	found, _ := tc.Get("float32", &x)
	if !found {
		t.Error("float32 was not found")
	}
	if x != 3.5 {
		t.Error("float32 is not 3:", x)
	}
}

func TestDecrementWithFloat64(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	var x float64
	tc.Set("float64", float64(5.5), DefaultExpiration)
	err := tc.Decrement("float64", 2)
	if err != nil {
		t.Error("Error decrementing:", err)
	}
	found, _ := tc.Get("float64", &x)
	if !found {
		t.Error("float64 was not found")
	}
	if x != 3.5 {
		t.Error("float64 is not 3:", x)
	}
}

func TestDecrementFloatWithFloat32(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	var x float32
	tc.Set("float32", float32(5.5), DefaultExpiration)
	err := tc.DecrementFloat("float32", 2)
	if err != nil {
		t.Error("Error decrementing:", err)
	}
	found, _ := tc.Get("float32", &x)
	if !found {
		t.Error("float32 was not found")
	}
	if x != 3.5 {
		t.Error("float32 is not 3:", x)
	}
}

func TestDecrementFloatWithFloat64(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	var x float64
	tc.Set("float64", float64(5.5), DefaultExpiration)
	err := tc.DecrementFloat("float64", 2)
	if err != nil {
		t.Error("Error decrementing:", err)
	}
	found, _ := tc.Get("float64", &x)
	if !found {
		t.Error("float64 was not found")
	}
	if x != 3.5 {
		t.Error("float64 is not 3:", x)
	}
}

func TestAdd(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	err := tc.Add("foo", "bar", DefaultExpiration)
	if err != nil {
		t.Error("Couldn't add foo even though it shouldn't exist")
	}
	err = tc.Add("foo", "baz", DefaultExpiration)
	if err == nil {
		t.Error("Successfully added another foo when it should have returned an error")
	}
}

func TestReplace(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	err := tc.Replace("foo", "bar", DefaultExpiration)
	if err == nil {
		t.Error("Replaced foo when it shouldn't exist")
	}
	tc.Set("foo", "bar", DefaultExpiration)
	err = tc.Replace("foo", "bar", DefaultExpiration)
	if err != nil {
		t.Error("Couldn't replace existing key foo")
	}
}

func TestDelete(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	var x string
	tc.Set("foo", "bar", DefaultExpiration)
	tc.Delete("foo")
	found, err := tc.Get("foo", &x)
	if found {
		t.Error("foo was found, but it should have been deleted")
	}
	if err != nil {
		t.Error(err)
	}
}

func TestItemCount(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	tc.Set("foo", "1", DefaultExpiration)
	tc.Set("bar", "2", DefaultExpiration)
	tc.Set("baz", "3", DefaultExpiration)
	if n := tc.ItemCount(); n != 3 {
		t.Errorf("Item count is not 3: %d", n)
	}
}

func TestFlush(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	var x string
	tc.Set("foo", "bar", DefaultExpiration)
	tc.Set("baz", "yes", DefaultExpiration)
	tc.Flush()
	found, err := tc.Get("foo", &x)
	if found {
		t.Error("foo was found, but it should have been deleted")
	}
	if err != nil {
		t.Error(err)
	}
	found, err = tc.Get("baz", &x)
	if found {
		t.Error("baz was found, but it should have been deleted")
	}
	if err != nil {
		t.Error(err)
	}
}

func TestIncrementOverflowInt(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	var x int8
	tc.Set("int8", int8(127), DefaultExpiration)
	err := tc.Increment("int8", 1)
	if err != nil {
		t.Error("Error incrementing int8:", err)
	}
	tc.Get("int8", &x)
	int8 := x
	if int8 != -128 {
		t.Error("int8 did not overflow as expected; value:", int8)
	}

}

func TestIncrementOverflowUint(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	var x uint8
	tc.Set("uint8", uint8(255), DefaultExpiration)
	err := tc.Increment("uint8", 1)
	if err != nil {
		t.Error("Error incrementing int8:", err)
	}
	tc.Get("uint8", &x)
	uint8 := x
	if uint8 != 0 {
		t.Error("uint8 did not overflow as expected; value:", uint8)
	}
}

func TestDecrementUnderflowUint(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	var x uint8
	tc.Set("uint8", uint8(0), DefaultExpiration)
	err := tc.Decrement("uint8", 1)
	if err != nil {
		t.Error("Error decrementing int8:", err)
	}
	tc.Get("uint8", &x)
	uint8 := x
	if uint8 != 255 {
		t.Error("uint8 did not underflow as expected; value:", uint8)
	}
}

func TestOnEvicted(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	var x int
	tc.Set("foo", 3, DefaultExpiration)
	if tc.onEvicted != nil {
		t.Fatal("tc.onEvicted is not nil")
	}
	works := false
	tc.OnEvicted(func(k string, v interface{}) {
		if k == "foo" && v.(int) == 3 {
			works = true
		}
		tc.Set("bar", 4, DefaultExpiration)
	})
	tc.Delete("foo")
	tc.Get("bar", &x)
	if !works {
		t.Error("works bool not true")
	}
	if x != 4 {
		t.Error("bar was not 4")
	}
}

func BenchmarkCacheGetExpiring(b *testing.B) {
	benchmarkCacheGet(b, 5*time.Minute)
}

func BenchmarkCacheGetNotExpiring(b *testing.B) {
	benchmarkCacheGet(b, NoExpiration)
}

func benchmarkCacheGet(b *testing.B, exp time.Duration) {
	b.StopTimer()
	var x string
	tc := NewMemoryCache(exp, 0)
	tc.Set("foo", "bar", DefaultExpiration)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		tc.Get("foo", &x)
	}
}

func BenchmarkRWMutexMapGet(b *testing.B) {
	b.StopTimer()
	m := map[string]string{
		"foo": "bar",
	}
	mu := sync.RWMutex{}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		mu.RLock()
		_, _ = m["foo"]
		mu.RUnlock()
	}
}

func BenchmarkRWMutexInterfaceMapGetStruct(b *testing.B) {
	b.StopTimer()
	s := struct{ name string }{name: "foo"}
	m := map[interface{}]string{
		s: "bar",
	}
	mu := sync.RWMutex{}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		mu.RLock()
		_, _ = m[s]
		mu.RUnlock()
	}
}

func BenchmarkRWMutexInterfaceMapGetString(b *testing.B) {
	b.StopTimer()
	m := map[interface{}]string{
		"foo": "bar",
	}
	mu := sync.RWMutex{}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		mu.RLock()
		_, _ = m["foo"]
		mu.RUnlock()
	}
}

func BenchmarkCacheGetConcurrentExpiring(b *testing.B) {
	benchmarkCacheGetConcurrent(b, 5*time.Minute)
}

func BenchmarkCacheGetConcurrentNotExpiring(b *testing.B) {
	benchmarkCacheGetConcurrent(b, NoExpiration)
}

func benchmarkCacheGetConcurrent(b *testing.B, exp time.Duration) {
	b.StopTimer()
	tc := NewMemoryCache(exp, 0)
	var x string
	tc.Set("foo", "bar", DefaultExpiration)
	wg := new(sync.WaitGroup)
	workers := runtime.NumCPU()
	each := b.N / workers
	wg.Add(workers)
	b.StartTimer()
	for i := 0; i < workers; i++ {
		go func() {
			for j := 0; j < each; j++ {
				tc.Get("foo", &x)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func BenchmarkRWMutexMapGetConcurrent(b *testing.B) {
	b.StopTimer()
	m := map[string]string{
		"foo": "bar",
	}
	mu := sync.RWMutex{}
	wg := new(sync.WaitGroup)
	workers := runtime.NumCPU()
	each := b.N / workers
	wg.Add(workers)
	b.StartTimer()
	for i := 0; i < workers; i++ {
		go func() {
			for j := 0; j < each; j++ {
				mu.RLock()
				_, _ = m["foo"]
				mu.RUnlock()
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func BenchmarkCacheGetManyConcurrentExpiring(b *testing.B) {
	benchmarkCacheGetManyConcurrent(b, 5*time.Minute)
}

func BenchmarkCacheGetManyConcurrentNotExpiring(b *testing.B) {
	benchmarkCacheGetManyConcurrent(b, NoExpiration)
}

func benchmarkCacheGetManyConcurrent(b *testing.B, exp time.Duration) {
	// This is the same as BenchmarkCacheGetConcurrent, but its result
	// can be compared against BenchmarkShardedCacheGetManyConcurrent
	// in sharded_test.go.
	b.StopTimer()
	n := 10000
	tc := NewMemoryCache(exp, 0)
	var x string
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		k := "foo" + strconv.Itoa(n)
		keys[i] = k
		tc.Set(k, "bar", DefaultExpiration)
	}
	each := b.N / n
	wg := new(sync.WaitGroup)
	wg.Add(n)
	for _, v := range keys {
		go func(k string) {
			for j := 0; j < each; j++ {
				tc.Get(k, &x)
			}
			wg.Done()
		}(v)
	}
	b.StartTimer()
	wg.Wait()
}

func BenchmarkCacheSetExpiring(b *testing.B) {
	benchmarkCacheSet(b, 5*time.Minute)
}

func BenchmarkCacheSetNotExpiring(b *testing.B) {
	benchmarkCacheSet(b, NoExpiration)
}

func benchmarkCacheSet(b *testing.B, exp time.Duration) {
	b.StopTimer()
	tc := NewMemoryCache(exp, 0)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		tc.Set("foo", "bar", DefaultExpiration)
	}
}

func BenchmarkRWMutexMapSet(b *testing.B) {
	b.StopTimer()
	m := map[string]string{}
	mu := sync.RWMutex{}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		mu.Lock()
		m["foo"] = "bar"
		mu.Unlock()
	}
}

func BenchmarkCacheSetDelete(b *testing.B) {
	b.StopTimer()
	tc := NewMemoryCache(DefaultExpiration, 0)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		tc.Set("foo", "bar", DefaultExpiration)
		tc.Delete("foo")
	}
}

func BenchmarkRWMutexMapSetDelete(b *testing.B) {
	b.StopTimer()
	m := map[string]string{}
	mu := sync.RWMutex{}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		mu.Lock()
		m["foo"] = "bar"
		mu.Unlock()
		mu.Lock()
		delete(m, "foo")
		mu.Unlock()
	}
}

func BenchmarkCacheSetDeleteSingleLock(b *testing.B) {
	b.StopTimer()
	tc := NewMemoryCache(DefaultExpiration, 0)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		tc.mu.Lock()
		tc.set("foo", "bar", DefaultExpiration)
		tc.delete("foo")
		tc.mu.Unlock()
	}
}

func BenchmarkRWMutexMapSetDeleteSingleLock(b *testing.B) {
	b.StopTimer()
	m := map[string]string{}
	mu := sync.RWMutex{}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		mu.Lock()
		m["foo"] = "bar"
		delete(m, "foo")
		mu.Unlock()
	}
}

func BenchmarkIncrementInt(b *testing.B) {
	b.StopTimer()
	tc := NewMemoryCache(DefaultExpiration, 0)
	tc.Set("foo", 0, DefaultExpiration)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		tc.Increment("foo", 1)
	}
}

func BenchmarkDeleteExpiredLoop(b *testing.B) {
	b.StopTimer()
	tc := NewMemoryCache(5*time.Minute, 0)
	tc.mu.Lock()
	for i := 0; i < 100000; i++ {
		tc.set(strconv.Itoa(i), "bar", DefaultExpiration)
	}
	tc.mu.Unlock()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		tc.DeleteExpired()
	}
}

func TestGetWithExpiration(t *testing.T) {
	tc := NewMemoryCache(DefaultExpiration, 0)
	var a interface{}
	found, expiration := tc.GetWithExpiration("a", &a)
	if found || a != nil || !expiration.IsZero() {
		t.Error("Getting A found value that shouldn't exist:", a)
	}

	var b interface{}
	found, expiration = tc.GetWithExpiration("b", &b)
	if found || b != nil || !expiration.IsZero() {
		t.Error("Getting B found value that shouldn't exist:", b)
	}

	var c interface{}
	found, expiration = tc.GetWithExpiration("c", &c)
	if found || c != nil || !expiration.IsZero() {
		t.Error("Getting C found value that shouldn't exist:", c)
	}

	tc.Set("a", 1, DefaultExpiration)
	tc.Set("b", "b", DefaultExpiration)
	tc.Set("c", 3.5, DefaultExpiration)
	tc.Set("d", 1, NoExpiration)
	tc.Set("e", 1, 50*time.Millisecond)

	var xa int
	found, expiration = tc.GetWithExpiration("a", &xa)
	if !found {
		t.Error("a was not found while getting a2")
	}
	if a2 := xa; a2+2 != 3 {
		t.Error("a2 (which should be 1) plus 2 does not equal 3; value:", a2)
	}
	if !expiration.IsZero() {
		t.Error("expiration for a is not a zeroed time")
	}

	var xb string
	found, expiration = tc.GetWithExpiration("b", &xb)
	if !found {
		t.Error("b was not found while getting b2")
	}
	if b2 := xb; b2+"B" != "bB" {
		t.Error("b2 (which should be b) plus B does not equal bB; value:", b2)
	}
	if !expiration.IsZero() {
		t.Error("expiration for b is not a zeroed time")
	}

	var xc float64
	found, expiration = tc.GetWithExpiration("c", &xc)
	if !found {
		t.Error("c was not found while getting c2")
	}
	if c2 := xc; c2+1.2 != 4.7 {
		t.Error("c2 (which should be 3.5) plus 1.2 does not equal 4.7; value:", c2)
	}
	if !expiration.IsZero() {
		t.Error("expiration for c is not a zeroed time")
	}

	var xd int
	found, expiration = tc.GetWithExpiration("d", &xd)
	if !found {
		t.Error("d was not found while getting d2")
	}
	if d2 := xd; d2+2 != 3 {
		t.Error("d (which should be 1) plus 2 does not equal 3; value:", d2)
	}
	if !expiration.IsZero() {
		t.Error("expiration for d is not a zeroed time")
	}

	var xe int
	found, expiration = tc.GetWithExpiration("e", &xe)
	if !found {
		t.Error("e was not found while getting e2")
	}
	if e2 := xe; e2+2 != 3 {
		t.Error("e (which should be 1) plus 2 does not equal 3; value:", e2)
	}
	if expiration.UnixNano() != tc.items["e"].Expiration {
		t.Error("expiration for e is not the correct time")
	}
	if expiration.UnixNano() < time.Now().UnixNano() {
		t.Error("expiration for e is in the past")
	}
}
