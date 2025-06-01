from fastapi import FastAPI, BackgroundTasks, HTTPException
from tortoise import fields, models
from tortoise.contrib.fastapi import register_tortoise
import time
from datetime import datetime
import httpx
import asyncio
import json

app = FastAPI()


class Task(models.Model):
    """Task model for storing task information."""
    task_id = fields.CharField(max_length=50, pk=True)
    status = fields.CharField(max_length=20, default="processing")
    progress = fields.FloatField(default=0)
    current_step = fields.IntField(default=0)
    total_steps = fields.IntField(default=150)  # 150 Pokemon
    started_at = fields.DatetimeField(auto_now_add=True)
    completed_at = fields.DatetimeField(null=True)
    error = fields.TextField(null=True)
    result_data = fields.TextField(null=True)  # Store Pokemon data as JSON string

    class Meta:
        table = "tasks"

    @property
    def result(self):
        """Get the result data as a Python object."""
        if self.result_data:
            return json.loads(self.result_data)
        return None

    @result.setter
    def result(self, value):
        """Set the result data as a JSON string."""
        if value is not None:
            self.result_data = json.dumps(value)
        else:
            self.result_data = None


async def get_active_tasks_count() -> int:
    """Get the number of currently active tasks."""
    return await Task.filter(status="processing").count()


async def process_task(task_id: str):
    """Process Pokemon data from PokeAPI."""
    total_pokemon = 150
    pokemon_data = []
    
    try:
        async with httpx.AsyncClient() as client:
            for number in range(1, total_pokemon + 1):
                try:
                    pokemon_url = f'https://pokeapi.co/api/v2/pokemon/{number}'
                    resp = await client.get(pokemon_url)
                    if resp.status_code == 200:
                        pokemon = resp.json()
                        pokemon_result = {
                            "id": pokemon["id"],
                            "name": pokemon["name"],
                            "types": [
                                t["type"]["name"] 
                                for t in pokemon["types"]
                            ]
                        }
                        print(pokemon_result)
                        pokemon_data.append(pokemon_result)
                    else:
                        raise Exception(
                            f"Failed to fetch Pokemon {number}"
                        )
                            
                    # Update progress
                    progress = (number / total_pokemon) * 100
                    await Task.filter(task_id=task_id).update(
                        progress=round(progress, 2),
                        current_step=number,
                        total_steps=total_pokemon,
                    )
                    
                    # Small delay to prevent rate limiting
                    await asyncio.sleep(0.1)
                    
                except Exception as e:
                    await Task.filter(task_id=task_id).update(
                        status="failed",
                        error=f"Error processing Pokemon {number}: {str(e)}",
                        completed_at=datetime.now()
                    )
                    raise
                    
        # Task completed successfully
        await Task.filter(task_id=task_id).update(
            status="completed",
            progress=100,
            completed_at=datetime.now(),
            result_data=json.dumps(pokemon_data)
        )
        
    except Exception as e:
        await Task.filter(task_id=task_id).update(
            status="failed",
            error=str(e),
            completed_at=datetime.now()
        )
        raise


async def run_background_task(task_id: str):
    """Run the process in background."""
    await Task.create(
        task_id=task_id,
        status="processing",
        progress=0,
        current_step=0,
        total_steps=150,
    )
    await process_task(task_id)


@app.post("/process/")
async def process_request(background_tasks: BackgroundTasks):
    """Endpoint that uses background tasks for processing."""
    active_tasks = await get_active_tasks_count()
    if active_tasks >= 10:
        raise HTTPException(
            status_code=429,
            detail="Maximum number of concurrent tasks (10) reached. "
                   "Please try again later."
        )

    task_id = f"task_{int(time.time())}"
    background_tasks.add_task(run_background_task, task_id)
    return {
        "task_id": task_id,
        "status": "processing",
        "message": "Pokemon data processing started in background",
        "estimated_duration": "~15 seconds",
        "active_tasks": active_tasks + 1,
    }


@app.get("/status/{task_id}")
async def get_task_status(task_id: str):
    """Get the status of a background task."""
    task = await Task.get_or_none(task_id=task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return {
        "task_id": task.task_id,
        "status": task.status,
        "progress": task.progress,
        "current_step": task.current_step,
        "total_steps": task.total_steps,
        "started_at": task.started_at.isoformat(),
        "completed_at": task.completed_at.isoformat() 
        if task.completed_at else None,
        "error": task.error,
        "result": task.result if task.status == "completed" else None
    }


@app.get("/tasks/")
async def list_tasks():
    """Get a list of all tasks with their IDs and statuses."""
    tasks = await Task.all()
    return [
        {"task_id": task.task_id, "status": task.status}
        for task in tasks
    ]


@app.get("/queue/status")
async def queue_status():
    """Get the current queue status."""
    active_tasks = await get_active_tasks_count()
    return {
        "active_tasks": active_tasks,
        "max_concurrent_tasks": 10,
        "available_slots": 10 - active_tasks
    }


# Tortoise ORM configuration
register_tortoise(
    app,
    db_url="sqlite://db.sqlite3",
    modules={"models": ["main"]},
    generate_schemas=True,
    add_exception_handlers=True,
)
