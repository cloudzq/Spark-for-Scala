//代码文件为/usr/local/scala/mycode/Counter.scala
class Counter {
    private var privateValue = 0
def value = privateValue
def value_=(newValue: Int){
        if (newValue > 0) privateValue = newValue
    }
def increment(step: Int): Unit = {value += step}
    def current():Int = {value}
}
