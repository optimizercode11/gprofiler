
        public class DemoApp {
            public static void main(String[] args) throws Exception {
                System.out.println("Demo App started. PID: " + java.lang.management.ManagementFactory.getRuntimeMXBean().getName());
                int i = 0;
                while (true) {
                    Thread.sleep(5000);
                    System.out.println("App running... iteration " + i++);

                    // Rename thread to trigger agent logic
                    Thread.currentThread().setName("demo-thread-" + i);
                }
            }
        }
