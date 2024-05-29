package com.kafkadelta.project
import org.apache.flink.api.common.functions.AggregateFunction

class SalesAmountAggregator extends  AggregateFunction[SalesOrderEvent, Int, Int]{
  override def createAccumulator(): Int = 0
  override def add(in: SalesOrderEvent, acc: Int): Int = acc + in.SalesAmount
  override def getResult(acc: Int): Int = acc
  override def merge(acc: Int, acc1: Int): Int = acc + acc1
}
