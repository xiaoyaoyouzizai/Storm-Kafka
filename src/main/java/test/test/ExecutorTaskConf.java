package test.test;

public class ExecutorTaskConf {

	public int executors = 1;

	public int getExecutors() {
		return executors;
	}

	// public void setExecutors(int executors) {
	// this.executors = executors;
	// }

	public int tasks = 1;

	public int getTasks() {
		return tasks;
	}

	// public void setTasks(int tasks) {
	// this.tasks = tasks;
	// }

	public ExecutorTaskConf(String executor_task_num) {
		String[] num = executor_task_num.split(":");
		executors = Utils.parseInt(num[0], 1);
		tasks = Utils.parseInt(num[1], 1);
	}
}
