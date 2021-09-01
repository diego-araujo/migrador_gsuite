import sys

class iCalCalendar:
    def __init__(self, ical):
        self.cal = ical

    def elements(self):
        ret = []
        for event in self.cal.components():
            if (event.name == 'VEVENT') and hasattr(event, 'summary') and hasattr(event, 'uid'):
                ret.append(event)
        return ret

        # Properly encode unicode characters.
        def encode_element(self, el):
            return el.encode('ascii', 'replace')

        def ical2gcal(self, e, dt):
            # Parse iCal event.
            event = {}
            event['uid'] = self.encode_element(dt.uid.value)
            event['subject'] = self.encode_element(dt.summary.value)
            if hasattr(dt, 'description') and (dt.description is not None):
                event['description'] = self.encode_element(dt.description.value)
            else:
                event['description'] = ''
            if hasattr(dt, 'location'):
                event['where'] = self.encode_element(dt.location.value)
            else:
                event['where'] = ''
            if hasattr(dt, 'status'):
                event['status'] = self.encode_element(dt.status.value)
            else:
                event['status'] = 'CONFIRMED'
            if hasattr(dt, 'organizer'):
                event['organizer'] = self.encode_element(dt.organizer.params['CN'][0])
                event['mailto'] = self.encode_element(dt.organizer.value)
                event['mailto'] = re.search('(?<=MAILTO:).+', event['mailto']).group(0)
            if hasattr(dt, 'rrule'):
                event['rrule'] = self.encode_element(dt.rrule.value)
            if hasattr(dt, 'dtstart'):
                event['start'] = dt.dtstart.value
            if hasattr(dt, 'dtend'):
                event['end'] = dt.dtend.value
            if hasattr(dt, 'valarm'):
                event['alarm'] = self.format_alarm(self.encode_element(dt.valarm.trigger.value))

            # Convert into a Google Calendar event.
            try:
                e.title = atom.Title(text=event['subject'])
                e.extended_property.append(gdata.calendar.ExtendedProperty(name='local_uid', value=event['uid']))
                e.content = atom.Content(text=event['description'])
                e.where.append(gdata.calendar.Where(value_string=event['where']))
                e.event_status = gdata.calendar.EventStatus()
                e.event_status.value = event['status']
                if event.has_key('organizer'):
                    attendee = gdata.calendar.Who()
                    attendee.rel = 'ORGANIZER'
                    attendee.name = event['organizer']
                    attendee.email = event['mailto']
                    attendee.attendee_status = gdata.calendar.AttendeeStatus()
                    attendee.attendee_status.value = 'ACCEPTED'
                    if len(e.who) > 0:
                        e.who[0] = attendee
                    else:
                        e.who.append(attendee)
                # TODO: handle list of attendees.
                if event.has_key('rrule'):
                    # Recurring event.
                    recurrence_data = ('DTSTART;VALUE=DATE:%s\r\n'
                                       + 'DTEND;VALUE=DATE:%s\r\n'
                                       + 'RRULE:%s\r\n') % ( \
                                          self.format_datetime_recurring(event['start']), \
                                          self.format_datetime_recurring(event['end']), \
                                          event['rrule'])
                    e.recurrence = gdata.calendar.Recurrence(text=recurrence_data)
                else:
                    # Single-occurrence event.
                    if len(e.when) > 0:
                        e.when[0] = gdata.calendar.When(start_time=self.format_datetime(event['start']), \
                                                        end_time=self.format_datetime(event['end']))
                    else:
                        e.when.append(gdata.calendar.When(start_time=self.format_datetime(event['start']), \
                                                          end_time=self.format_datetime(event['end'])))
                    if event.has_key('alarm'):
                        # Set reminder.
                        for a_when in e.when:
                            if len(a_when.reminder) > 0:
                                a_when.reminder[0].minutes = event['alarm']
                            else:
                                a_when.reminder.append(gdata.calendar.Reminder(minutes=event['alarm']))
            except Exception as e:
                logger.exception("ical2gcal" + 'ERROR: couldn\'t create gdata event object: ', event['subject'])
                return False

            # Use the Google-compliant datetime format for single events.
            def format_datetime(self, date):
                try:
                    if re.match(r'^\d{4}-\d{2}-\d{2}$', str(date)):
                        return str(date)
                    else:
                        return str(time.strftime("%Y-%m-%dT%H:%M:%S.000Z", date.utctimetuple()))
                except:
                    return str(date)

            # Use the Google-compliant datetime format for recurring events.
            def format_datetime_recurring(self, date):
                try:
                    if re.match(r'^\d{4}-\d{2}-\d{2}$', str(date)):
                        return str(date).replace('-', '')
                    else:
                        return str(time.strftime("%Y%m%dT%H%M%SZ", date.utctimetuple()))
                except:
                    return str(date)

            # Use the Google-compliant alarm format.
            def format_alarm(self, alarm):
                google_minutes = [5, 10, 15, 20, 25, 30, 45, 60, 120, 180, 1440, 2880, 10080]
                m = re.match('-(\d+)( day[s]?, )?(\d+):(\d{2}):(\d{2})', alarm)
                try:
                    time = m.groups()
                    t = 60 * ((int(time[0]) - 1) * 24 + (23 - int(time[2]))) + (60 - int(time[3]))
                    # Find the closest minutes value valid for Google.
                    closest_min = google_minutes[0]
                    closest_diff = sys.maxint
                    for m in google_minutes:
                        diff = abs(t - m)
                        if diff == 0:
                            return m
                        if diff < closest_diff:
                            closest_min = m
                            closest_diff = diff
                    return closest_min
                except:
                    return 0

