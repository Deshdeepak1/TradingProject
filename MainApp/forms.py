from django import forms


class CsvForm(forms.Form):
    csv_file = forms.FileField()
    timeframe = forms.IntegerField()
