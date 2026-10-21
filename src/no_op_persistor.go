package paxocache

type NoOpPersistor[TOp comparable, TRes any] struct {

}

func (this *NoOpPersistor[TOp, TRes]) WriteSingleData(data *PaxosSingleData) error {
	return nil
}
	
func (this *NoOpPersistor[TOp, TRes]) AppendPoolOp(op *PoolChangeOp) error {
	return nil
}
	
func (this *NoOpPersistor[TOp, TRes]) AppendAcceptance(slot *Slot[TOp]) error {
	return nil
}
	
func (this *NoOpPersistor[TOp, TRes]) AppendChosen(slot *ChosenSlot[TOp]) error {
	return nil
}
	
func (this *NoOpPersistor[TOp, TRes]) HasData() (bool, error) {
	return false, nil
}
	
func (this *NoOpPersistor[TOp, TRes]) Recover() (*PaxosPersistentData[TOp, TRes], error) {
	return nil, nil
}