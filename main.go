package main

import (
	"fmt"
	"github.com/scritchley/orc"
)

func main() {
	r := readORC("./files/data-pipeline-prod-1-events-3-2019-01-03-00-58-04-0f368b58-3626-465e-a023-dbe0d3cc73a3.orc")

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
		var s []string
		c := r.Select(i)

		for c.Stripes() {
		
			// Iterate over each row in the stripe.
			for c.Next() {	
				// Retrieve a slice of interface values for the current row.
				s = append(s, c.Row()[0].(string))
				
			}
		
		}

		fmt.Println("Field: ", i)
		fmt.Println("Data: ", s)
		
		if err := c.Err(); err != nil {
			fmt.Println(err)
		}
	}
}