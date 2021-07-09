package blocking

func Equal(o1, o2 interface{}) bool {
	if o1 == o2 {
		return true
	}
	e1, ok := o1.(interface {
		Equal(o interface{}) bool
	})
	if !ok {
		return false
	}
	return e1.Equal(o2)
}
