A = load '/user/kitenga/pig/weather/input/weather2.txt' USING PigStorage(',') as (a0:int, a1:int, a2:int);
B = order A by a0;
C = limit B 2;
store C into '/user/kitenga/pig/weather/output2';
