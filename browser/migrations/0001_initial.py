# Generated by Django 4.1 on 2023-03-03 01:03

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='TitleAkas',
            fields=[
                ('titleID', models.CharField(max_length=100, primary_key=True, serialize=False)),
            ],
            options={
                'db_table': 'titleakas',
            },
        ),
    ]
