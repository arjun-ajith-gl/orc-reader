package main

import (
	"fmt"
	"github.com/scritchley/orc"
	"math/rand"
	"os"
	"time"
)

func main() {
	writeORC()

	r := readORC("test.orc")

	getSchema(r)
	readData(r)

}

func readORC(path string) *orc.Reader {
	r, err := orc.Open(path)
	if err != nil {
		fmt.Println("error while trying to create reader: ", err)
	}
	return r
}

func getSchema(r *orc.Reader) {
	fmt.Println(r.Schema())
}

func readData(r *orc.Reader) {
	col := r.Schema().Columns()
	for _, i := range col {
		var s []interface{}
		c := r.Select(i)

		for c.Stripes() {

			// Iterate over each row in the stripe.
			for c.Next() {
				// Retrieve a slice of interface values for the current row.
				s = append(s, c.Row()[0])

			}

		}

		fmt.Println("Field: ", i)
		fmt.Println("Data: ", s)

		if err := c.Err(); err != nil {
			fmt.Println(err)
		}
	}
}

func writeORC() {

	f, err := os.Create("test.orc")
	if err != nil {
		fmt.Println(err)
	}

	filename := f.Name()
	fmt.Println("File Name: ", filename)
	// defer os.Remove(filename) // clean up
	defer f.Close()

	schema, err := orc.ParseSchema("struct<string1:string,timestamp1:timestamp,int1:int,boolean1:boolean,double1:double,nested:struct<double2:double,nested:struct<int2:int>>>")
	if err != nil {
		fmt.Println(err)
	}

	w, err := orc.NewWriter(f, orc.SetSchema(schema))
	if err != nil {
		fmt.Println(err)
	}

	now := time.Unix(1478123411, 99).UTC()
	timeIncrease := 5*time.Second + 10001*time.Nanosecond
	length := 1000
	var intSum int64
	for i := 0; i < length; i++ {
		string1 := fmt.Sprintf("%x", rand.Int63n(1000))
		timestamp1 := now.Add(time.Duration(i) * timeIncrease)
		int1 := rand.Int63n(10000)
		intSum += int1
		boolean1 := int1 > 4444
		double1 := rand.Float64()
		nested := []interface{}{
			rand.Float64(),
			[]interface{}{
				rand.Int63n(10000),
			},
		}
		err = w.Write(string1, timestamp1, int1, boolean1, double1, nested)
		if err != nil {
			fmt.Println(err)
		}
	}

	err = w.Close()
	if err != nil {
		fmt.Println(err)
	}
}
