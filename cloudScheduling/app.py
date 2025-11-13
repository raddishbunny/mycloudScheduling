from flask import Flask, render_template, request, jsonify
from pymongo import MongoClient

app = Flask(__name__)

# MongoDB Connection
client = MongoClient("mongodb://localhost:27017/")
db = client["cloud_scheduler"]
resources_col = db["resources"]
tasks_col = db["tasks"]

@app.route('/')
def home():
    return render_template('index.html')


# -------------------- ADD RESOURCE --------------------
@app.route('/add_resource', methods=['POST'])
def add_resource():
    data = request.json
    resource = {
        "vm_id": data.get("vm_id"),
        "cpu_cores": int(data.get("cpu_cores", 0)),
        "ram_mb": int(data.get("ram_mb", 0)),
        "storage_gb": int(data.get("storage_gb", 0)),
        "energy_rate": float(data.get("energy_rate", 0)),
        "status": "available",
        "current_load": 0
    }
    # avoid duplicate vm_id
    if resources_col.find_one({"vm_id": resource["vm_id"]}):
        return jsonify({"message": "VM with this ID already exists."}), 400

    resources_col.insert_one(resource)
    return jsonify({"message": "Resource added successfully!"})


# -------------------- ADD TASK --------------------
@app.route('/add_task', methods=['POST'])
def add_task():
    data = request.json
    task = {
        "task_id": data.get("task_id"),
        "cpu_required": int(data.get("cpu_required", 0)),
        "ram_required_mb": int(data.get("ram_required_mb", 0)),
        "disk_storage_mb": int(data.get("disk_storage_mb", 0)),
        "user_type": data.get("user_type", "regular"),
        "time_required_sec": int(data.get("time_required_sec", 10)),
        "status": "pending"
    }
    # avoid duplicate task_id
    if tasks_col.find_one({"task_id": task["task_id"]}):
        return jsonify({"message": "Task with this ID already exists."}), 400

    tasks_col.insert_one(task)
    return jsonify({"message": "Task added successfully!"})


# -------------------- SCHEDULING LOGIC --------------------
@app.route('/schedule', methods=['POST'])
def schedule_tasks():
    # Fetch VMs and pending tasks
    all_vms = list(resources_col.find())
    pending_tasks = list(tasks_col.find({"status": "pending"}))

    if not all_vms:
        return jsonify({"message": "No VMs available in the system."}), 400
    if not pending_tasks:
        return jsonify({"message": "No pending tasks to schedule."}), 400

    # Determine algorithm
    avg_time = sum(task.get("time_required_sec", 10) for task in pending_tasks) / len(pending_tasks)
    min_time = min(task.get("time_required_sec", 10) for task in pending_tasks)
    max_time = max(task.get("time_required_sec", 10) for task in pending_tasks)
    priorities_present = any("user_type" in task and task["user_type"] != "regular" for task in pending_tasks)
    high_variance_time = (max_time - min_time) > 10

    if priorities_present:
        algorithm = "priority"
    elif avg_time < 15 and not high_variance_time:
        algorithm = "sjf"
    elif high_variance_time:
        algorithm = "rr"
    else:
        algorithm = "fcfs"

    # Sorting logic
    if algorithm == "sjf":
        pending_tasks.sort(key=lambda x: x.get("time_required_sec", 10))
    elif algorithm == "priority":
        priority_map = {"vip": 3, "premium": 2, "regular": 1}
        pending_tasks.sort(
            key=lambda x: priority_map.get(x.get("user_type", "regular"), 1),
            reverse=True
        )

    scheduled_tasks = []
    rr_index = 0

    # Assign tasks one by one
    for task in pending_tasks:
        assigned_vm = None

        # STEP 1: Try free VMs first
        available_vms = [vm for vm in all_vms if vm.get("status") == "available"]
        for vm in available_vms:
            if (
                vm["cpu_cores"] >= task["cpu_required"] and
                vm["ram_mb"] >= task["ram_required_mb"] and
                vm["storage_gb"] * 1024 >= task["disk_storage_mb"]
            ):
                assigned_vm = vm
                break

        # STEP 2: If no free VM, try a partially loaded one
        if not assigned_vm:
            for vm in all_vms:
                used_load = vm.get("current_load", 0)
                remaining_cpu = vm["cpu_cores"] - used_load
                if (
                    remaining_cpu >= task["cpu_required"] and
                    vm["ram_mb"] >= task["ram_required_mb"] and
                    vm["storage_gb"] * 1024 >= task["disk_storage_mb"]
                ):
                    assigned_vm = vm
                    break

        # STEP 3: Assign and update resource usage
        if assigned_vm:
            new_load = assigned_vm.get("current_load", 0) + task["cpu_required"]
            new_status = "busy" if new_load < assigned_vm["cpu_cores"] else "full"

            # Update MongoDB
            tasks_col.update_one(
                {"task_id": task["task_id"]},
                {"$set": {"status": "running", "vm_id": assigned_vm["vm_id"]}}
            )
            resources_col.update_one(
                {"vm_id": assigned_vm["vm_id"]},
                {"$set": {"current_load": new_load, "status": new_status}}
            )

            scheduled_tasks.append({
                "task_id": task["task_id"],
                "vm_id": assigned_vm["vm_id"],
                "time_required_sec": task.get("time_required_sec", 10)
            })

    if scheduled_tasks:
        return jsonify({
            "scheduled": scheduled_tasks,
            "algorithm_used": algorithm,
            "message": "Tasks scheduled successfully with load-based allocation."
        })
    else:
        return jsonify({"message": "No tasks could be scheduled. All VMs overloaded or insufficient capacity."}), 400


# -------------------- COMPLETE TASK --------------------
@app.route('/complete_task', methods=['POST'])
def complete_task():
    """
    Marks a task as completed and updates VM load accordingly.
    """
    data = request.json or {}
    task_id = data.get("task_id")
    vm_id = data.get("vm_id")
    if not task_id or not vm_id:
        return jsonify({"message": "task_id and vm_id required"}), 400

    # Fetch task info
    task = tasks_col.find_one({"task_id": task_id})
    if not task:
        return jsonify({"message": "Task not found."}), 404

    # Fetch VM info
    vm = resources_col.find_one({"vm_id": vm_id})
    if not vm:
        return jsonify({"message": "VM not found."}), 404

    # Calculate new load
    used_load = vm.get("current_load", 0)
    new_load = max(0, used_load - task.get("cpu_required", 0))
    new_status = "available" if new_load == 0 else "busy"

    # Update DB
    tasks_col.update_one({"task_id": task_id}, {"$set": {"status": "completed"}})
    resources_col.update_one(
        {"vm_id": vm_id},
        {"$set": {"current_load": new_load, "status": new_status}}
    )

    return jsonify({"message": f"Task {task_id} completed. VM {vm_id} load updated."})


# -------------------- FETCH DATA --------------------
@app.route('/resources', methods=['GET'])
def get_resources():
    resources = list(resources_col.find())
    for r in resources:
        r["_id"] = str(r["_id"])
    return jsonify(resources)


@app.route('/tasks', methods=['GET'])
def get_tasks():
    tasks = list(tasks_col.find())
    for t in tasks:
        t["_id"] = str(t["_id"])
    return jsonify(tasks)
@app.route("/delete_resource", methods=["POST"])
def delete_resource():
    data = request.get_json()
    vm_id = data.get("vm_id")
    if not vm_id:
        return jsonify({"message": "VM ID missing"}), 400
    db.resources.delete_one({"vm_id": vm_id})
    db.assignments.delete_many({"vm_id": vm_id})
    return jsonify({"message": f"Resource {vm_id} deleted"}), 200

@app.route("/delete_task", methods=["POST"])
def delete_task():
    data = request.get_json()
    task_id = data.get("task_id")
    if not task_id:
        return jsonify({"message": "Task ID missing"}), 400
    db.tasks.delete_one({"task_id": task_id})
    db.assignments.delete_many({"task_id": task_id})
    return jsonify({"message": f"Task {task_id} deleted"}), 200


# -------------------- RUN SERVER --------------------
if __name__ == "__main__":
    app.run(debug=True, port=5000)
