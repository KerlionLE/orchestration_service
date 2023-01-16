def create_task_run_config(task_id, **config):
	task = objects.Task.get(id=task_id)
	config_template = task.config_template

	return config_template.format(**config)


def create_task_run(task_id, **config):
	if task_id is not None:
		return TaskRun(
			task=task_id,
			status=None,
			config=create_task_run_config(task_id, **config),
			result=None
		)


def get_next_task_runs(previous_task_run=None):
	previous_task_id = None
	previous_task_run_result = dict()

	if previous_task_run is not None:
		previous_task_id = previous_task_run.task.id
		previous_task_run_result = previous_task_run.result

	chains = objects.Chain.filter(prev_task=previous_task_id)

	return [
		create_task_run(
			task_id=chain.next_task,
			**previous_task_run_result
		) for chain in chains
	] # в списке могут быть None


6 -> 7 -> [8, 9]



1 -> 2
[1, 5] -> 3
1 -> 4

chain
_id prv nxt
  1   1   2
  2   1   3
  3   1   4
  4   5   3
  5   6   7
  6   7   8
  7   7   9

graph (integration)
_id name
  1  ...

graph_chain
graph chain
    1     1
    1     2
    1     3
    1     4
    2     1 (ошибка - нужна проверка)

graph_run
_id created_dt updated_dt status
  1        ...        ...    ... # Нужен демон, который будет ставить статусы FAILED для незавершившихся run




4 -> 6
