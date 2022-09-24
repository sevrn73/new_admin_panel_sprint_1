import uuid
from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from django.utils.translation import gettext_lazy as _


class TimeStampedMixin(models.Model):
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class UUIDMixin(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:
        abstract = True


class Genre(UUIDMixin, TimeStampedMixin):
    name = models.CharField(_('name'), max_length=255)
    description = models.TextField(_('description'), blank=True)

    class Meta:
        db_table = 'content"."genre'
        verbose_name = _('Genre')
        verbose_name_plural = _('Genres')

    def __str__(self):
        return self.name


class GenreFilmwork(UUIDMixin):
    film_work = models.ForeignKey('Filmwork', on_delete=models.CASCADE)
    genre = models.ForeignKey('Genre', on_delete=models.CASCADE)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'content"."genre_film_work'


class Person(UUIDMixin, TimeStampedMixin):
    full_name = models.CharField(_('full_name'), unique=True, max_length=255)

    class Meta:
        db_table = 'content"."person'
        verbose_name = _('Person')
        verbose_name_plural = _('Persons')

    def __str__(self):
        return self.full_name


class PersonFilmwork(UUIDMixin):
    film_work = models.ForeignKey('Filmwork', on_delete=models.CASCADE)
    person = models.ForeignKey('Person', on_delete=models.CASCADE)
    role = models.TextField(_('role'), null=True)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'content"."person_film_work'


class Filmwork(UUIDMixin, TimeStampedMixin):
    title = models.CharField(_('title'), max_length=255)
    description = models.TextField(_('description'), blank=True)
    creation_date = models.DateTimeField(_('creation_date'), blank=True)
    rating = models.FloatField(_('rating'), validators=[MinValueValidator(0), MaxValueValidator(100)], blank=True)
    certificate = models.CharField(_('certificate'), max_length=512, blank=True)
    file_path = models.FileField(_('file'), blank=True, null=True, upload_to='movies/')
    genres = models.ManyToManyField(Genre, through='GenreFilmwork')
    preson = models.ManyToManyField(Person, through='PersonFilmwork')

    class FilmworkType(models.TextChoices):
        MOVIE = 'mo', _('movie')
        TV_SHOW = 'tv', _('tv_show')

    type = models.CharField('Тип', choices=FilmworkType.choices, default=FilmworkType.MOVIE, max_length=2)

    class Meta:
        db_table = 'content"."film_work'
        verbose_name = _('Film_work')
        verbose_name_plural = _('Film_works')

    def __str__(self):
        return f'{self.title} {self.creation_date} {self.rating}'