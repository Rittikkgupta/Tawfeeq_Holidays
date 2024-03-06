from django.db import models

# Create your models here.
from django.db import models

class User(models.Model):
    username = models.CharField(max_length=255)
    userpwd = models.CharField(max_length=255)  # Assuming you store encrypted passwords
