from celery import Celery

# Конфигурация Celery
app = Celery(
    'project_name',
    broker='redis://localhost:6379/0',  # Настроим Redis как брокера
    backend='redis://localhost:6379/1'
)

# Загрузка настроек из файла config
app.config_from_object('config', namespace='CELERY')

# Автоматический поиск задач в проекте
app.autodiscover_tasks()
