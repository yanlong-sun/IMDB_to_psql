from django.db import models
from django.contrib.postgres.fields import ArrayField
# Create your models here.


class TitleAkas(models.Model):
   class Meta:
       db_table = 'titleakas'
   titleID = models.CharField(max_length=100, primary_key=True)
   """
   ordering = models.IntegerField()
   title = models.CharField(max_length=32)
   region = models.CharField(max_length=32)
   language = models.CharField(max_length=32)
   types = ArrayField(models.CharField(max_length=50))
   attributes = models.JSONField()
   isOriginalTitle = models.BooleanField(null=False)
   """
