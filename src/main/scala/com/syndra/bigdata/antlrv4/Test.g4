// grammar 后的名字必须与文件名相同
grammar Test;

tsinit : '{' value (',' value)* '}';

value : INT
      | tsinit;

// 可以是 0-9的一个或多个数字
INT : [0-9]+;
// 忽略一个或多个空白字符
WS : [ \t\r\n]+ -> skip;
