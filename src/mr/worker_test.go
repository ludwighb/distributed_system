package mr

import (
	"reflect"
	"testing"
)

func TestGroupByKey(t *testing.T) {

}

func TestSplitKeyValuesToIntermediateFiles(t *testing.T) {
	testCases := []struct {
		kevValues                   []*KeyValues
		nReduce                     int
		nMapTaskID                  int
		wantIntermediateFileContent map[string][]*KeyValues
	}{
		{
			kevValues: []*KeyValues{
				{
					Key:    "A",
					Values: []string{"1", "1"},
				},
				{
					Key:    "B",
					Values: []string{"1"},
				},

				{
					Key:    "About",
					Values: []string{"1", "1", "1", "1"},
				},
			},
			nReduce:    3,
			nMapTaskID: 10,
			wantIntermediateFileContent: map[string][]*KeyValues{
				"mr-10-0.json": {
					{
						Key:    "About",
						Values: []string{"1", "1", "1", "1"},
					},
				},
				"mr-10-1.json": {
					{
						Key:    "A",
						Values: []string{"1", "1"},
					},
					{
						Key:    "B",
						Values: []string{"1"},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			got := splitKeyValuesToIntermediateFiles(tc.kevValues, tc.nReduce, tc.nMapTaskID)
			if !reflect.DeepEqual(got, tc.wantIntermediateFileContent) {
				t.Errorf("splitKeyValuesToIntermediateFiles returned wrong values! got: %v, want: %v", got, tc.wantIntermediateFileContent)
			}
		})
	}
}
