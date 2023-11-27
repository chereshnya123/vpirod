#!/bin/bash
leaders_amount=$1

for ((i = 1; i < leaders_amount; i++))
do
    rabbitmqadmin delete queue name=master$i
done

rabbitmqadmin delete queue name=master
rabbitmqadmin delete queue name=master0
rabbitmqadmin delete queue name=simulator

rm -rf dump.vars

rabbitmqadmin list queues
