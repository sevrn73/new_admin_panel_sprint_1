# Generated by Django 4.1.1 on 2022-09-25 13:25

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('movies', '0003_filmwork_certificate'),
    ]

    operations = [
        migrations.AlterField(
            model_name='filmwork',
            name='creation_date',
            field=models.DateTimeField(
                blank=True, null=True, verbose_name='creation_date'
            ),
        ),
    ]