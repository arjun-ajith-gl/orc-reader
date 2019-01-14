package main

import (
	"fmt"
	"github.com/scritchley/orc"
)

func main() {
	r, err := orc.Open("./files/data-pipeline-prod-1-events-3-2019-01-03-00-58-04-0f368b58-3626-465e-a023-dbe0d3cc73a3.orc")
	if err != nil {
		fmt.Println("error while trying to create reader: ", err)
		return
	}
	//fmt.Println(r.Schema())
	col := r.Schema().Columns()
	for _, i := range col {
		var s []string
		c := r.Select(i)
		fmt.Println("Field: ", i)
		for c.Stripes() {
		
			// Iterate over each row in the stripe.
			for c.Next() {
				
				// Retrieve a slice of interface values for the current row.
				s = append(s, c.Row()[0].(string))
				
				
			}
		
		}
		fmt.Println("Data: ", s)
		return
		
		if err := c.Err(); err != nil {
			fmt.Println(err)
		}
	}

	// fmt.Println("sdsdfsdfds: ", rows)

	// fmt.Println("sdsdfsdfds: ", r.Schema().Columns())


	// rows := r.Select("_col0")
	// fmt.Println(rows)

}