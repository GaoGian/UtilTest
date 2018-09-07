/**
 * Created by tingyun on 2018/6/14.
 */
public class MyTest {

//    @Test
    public void test(){
        AgentServerValue agentServerValue1 = new AgentServerValue("test1", 111L, 222L, 100);
        AgentServerValue agentServerValue2 = new AgentServerValue("test2", 333L, 444L, 100);

        System.out.println("JDK version：" + System.getProperty("java.version") + ", 比较结果：" + agentServerValue1.compareTo(agentServerValue2));
    }

    public static void main(String[] args){
        System.out.println(System.getProperty("os.name"));
        System.out.println(System.getProperty("os.arch"));
    }

}
