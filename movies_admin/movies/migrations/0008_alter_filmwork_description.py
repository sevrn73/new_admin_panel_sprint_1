# Generated by Django 4.1.1 on 2022-09-25 14:05

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('movies', '0007_alter_filmwork_type'),
    ]

    operations = [
        migrations.AlterField(
            model_name='filmwork',
            name='description',
            field=models.TextField(blank=True, null=True, verbose_name='description'),
        ),
    ]