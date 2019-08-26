# -*- coding: utf-8 -*-

# Программа для демонстрации работы датчика ОДТиТ
# Подключается к x55, настраивает потоковую передачу
# Получает данные, усредняет и складывает в БД

import asyncio
import logging
import hyperion_python3
import datetime
import OptenFiberOpticDevices
import instrument_description


instrument_descr = instrument_description.si255_instrument
instrument_description_hash = 100

# буфер для хранения неусредненных измерений
raw_measurements_buffer = dict()
raw_measurements_buffer['is_locked'] = False

wls_buffer = dict()
wls_buffer['is_locked'] = False

out_file_name = 'data.txt'

# тут храним усредненные измерения
averaged_measurements = dict()

# описание всех устройств
devices = list()

h1 = None


def get_one_block(h1, instrument_info):
    """ Функция получает один блок усредненных измерений со всех измерителей, описанных в si255_instrument_info """

    si255_result = list()

    sn = h1.get_serial_number()

    return si255_result


def return_error(e):
    """ функция принимает все ошибки программы, передает их на сервер"""
    print(e)
    return None


def instrument_init(instrument_description):
    """ функция инициализирует x55 """

    # получаем адрес x55
    instrument_ip = instrument_description['IP_address']
    if not isinstance(instrument_ip, str):
        instrument_ip = instrument_ip[0]

    # соединяемся с x55
    h1 = None
    while not h1:
        try:
            h1 = hyperion_python3.Hyperion(instrument_ip)
        except hyperion_python3.HyperionError as e:
            return_error(e)
            return None

    while not h1.is_ready():
        pass

    print(u'Instrument name: ' + h1.get_instrument_name())
    print(u'Instrument sn: ' + h1.get_serial_number()['content'])

    # ToDo настраиваем параметры PeakDetection
    peak_detection_settings = instrument_description['DetectionSettings']
    print(peak_detection_settings)

    # instrument time settings
    h1.set_ntp_enabled(False)
    local_UTC_time = datetime.datetime.utcnow()
    h1.set_instrument_utc_date_time(local_UTC_time.year, local_UTC_time.month, local_UTC_time.day, local_UTC_time.hour, local_UTC_time.minute, local_UTC_time.second)

    # включаем все каналы, на которых есть решетки
    active_channels = set()
    for device in instrument_description['devices']:
        active_channels.add(int(device['x55_channel']))
    try:
        h1.set_active_full_spectrum_channel_numbers(active_channels)
    except hyperion_python3.HyperionError as e:
        return_error(e)
        return None

    # запускаем!
    h1.enable_spectrum_streaming()
    h1.enable_peak_streaming()

    return h1


async def get_data_from_x55_coroutine():
    """ получение данных с x55 и сохранение их в буфер """
    global wls_buffer, devices, h1

    # если нет информации об инструменте, то не можем получать данные
    while not instrument_descr:
        await asyncio.sleep(0.1)

    index_of_reflection = 1.4682
    speed_of_light = 299792458.0

    # вытаскиваем информацию об устройствах
    for device_description in instrument_descr['devices']:

        # ToDo перенести это в класс ODTiT
        device = None
        try:
            device = OptenFiberOpticDevices.ODTiT(device_description['x55_channel'])
            device.id = device_description['ID']
            device.name = device_description['Name']
            device.channel = device_description['x55_channel']
            device.ctes = device_description['CTES']
            device.e = device_description['E']
            device.size = (device_description['Asize'], device_description['Bsize'])
            device.t_min = device_description['Tmin']
            device.t_max = device_description['Tmax']
            device.f_min = device_description['Fmin']
            device.f_max = device_description['Fmax']
            device.f_reserve = device_description['Freserve']
            device.span_rope_diameter = device_description['SpanRopeDiametr']
            device.span_len = device_description['SpanRopeLen']
            device.span_rope_density = device_description['SpanRopeDensity']
            device.span_rope_EJ = device_description['SpanRopeEJ']
            device.bend_sens = device_description['Bending_sensivity']
            device.time_of_flight = int(-2E9 * device_description['Distance'] * index_of_reflection / speed_of_light)

            device.sensors[0].id = device_description['Sensor4100']['ID']
            device.sensors[0].type = device_description['Sensor4100']['type']
            device.sensors[0].name = device_description['Sensor4100']['name']
            device.sensors[0].wl0 = device_description['Sensor4100']['WL0']
            device.sensors[0].t0 = device_description['Sensor4100']['T0']
            device.sensors[0].p_max = device_description['Sensor4100']['Pmax']
            device.sensors[0].p_min = device_description['Sensor4100']['Pmin']
            device.sensors[0].st = device_description['Sensor4100']['ST']

            device.sensors[1].id = device_description['Sensor3110_1']['ID']
            device.sensors[1].type = device_description['Sensor3110_1']['type']
            device.sensors[1].name = device_description['Sensor3110_1']['name']
            device.sensors[1].wl0 = device_description['Sensor3110_1']['WL0']
            device.sensors[1].t0 = device_description['Sensor3110_1']['T0']
            device.sensors[1].p_max = device_description['Sensor3110_1']['Pmax']
            device.sensors[1].p_min = device_description['Sensor3110_1']['Pmin']
            device.sensors[1].fg = device_description['Sensor3110_1']['FG']
            device.sensors[1].ctet = device_description['Sensor3110_1']['CTET']

            device.sensors[2].id = device_description['Sensor3110_2']['ID']
            device.sensors[2].type = device_description['Sensor3110_2']['type']
            device.sensors[2].name = device_description['Sensor3110_2']['name']
            device.sensors[2].wl0 = device_description['Sensor3110_2']['WL0']
            device.sensors[2].t0 = device_description['Sensor3110_2']['T0']
            device.sensors[2].p_max = device_description['Sensor3110_2']['Pmax']
            device.sensors[2].p_min = device_description['Sensor3110_2']['Pmin']
            device.sensors[2].fg = device_description['Sensor3110_2']['FG']
            device.sensors[2].ctet = device_description['Sensor3110_2']['CTET']

        except KeyError as e:
            return_error('JSON error - key ' + str(e) + ' did not find')

        devices.append(device)

    # все каналы, на которых есть решетки
    active_channels = set()
    for device in devices:
        active_channels.add(int(device.channel))

    # инициализация x55 - пока не соединимся
    h1 = instrument_init(instrument_descr)
    while not h1:
        await asyncio.sleep(hyperion_python3.DEFAULT_TIMEOUT / 1000)
        h1 = instrument_init(instrument_descr)
    logging.info(u'x55 has been initializated')

    wavelength_start = h1.get_wavelength_start()
    wavelength_delta = h1.get_wavelength_delta()
    wavelength_finish = wavelength_start + h1.get_wavelength_number_of_points() * h1.get_wavelength_delta()

    while True:
        await asyncio.sleep(0.01)

        all_peaks = h1.get_peaks()
        # spectrum = h1.get_spectrum()
        measurement_time = h1.peaksHeader.timeStampInt + h1.peaksHeader.timeStampFrac * 1E-9
        # print(measurement_time, all_peaks)

        # ждем освобождения буфера
        while wls_buffer['is_locked']:
            await asyncio.sleep(0.01)

        # блокируем буфер для записи в него
        wls_buffer['is_locked'] = True
        try:
            wls_buffer.setdefault(measurement_time, all_peaks)
            # logging.info(u'WLs saved')

        finally:
            # разблокируем буфер
            wls_buffer['is_locked'] = False
        continue


async def convert_wl_to_device():
    global raw_measurements_buffer

    while True:
        await asyncio.sleep(0.1)

        # ждем появления данных в буфере
        while len(wls_buffer.items()) < 2:
            await asyncio.sleep(1)

        # ждем освобождения буфера
        while wls_buffer['is_locked']:
            await asyncio.sleep(0.1)

        # блокируем буфер (чтобы надежно прочитать его)
        wls_buffer['is_locked'] = True
        try:
            # по всем записям в raw_measurements_buffer
            for (measurement_time, all_peaks) in wls_buffer.items():
                if measurement_time == 'is_locked':
                    continue

                for device in devices:
                    # переводим пики в пикометры, а также компенсируем все пики по расстоянию до текущего устройтва
                    wls_pm = all_peaks.get_channel(device.channel)
                    wls_pm = list(map(lambda wl: wl*1000, wls_pm))

                    # среди всех пиков ищем 3 подходящих для теукущего измерителя
                    wls = device.find_yours_wls(wls_pm, device.channel)

                    # если все три пика измерителя нашлись, то вычисляем тяжения и пр. Нет - вставляем пустышки
                    if wls:
                        device_output = device.get_tension_fav_ex(wls[1], wls[2], wls[0])
                        device_output.setdefault('WL_T', wls[0])
                        device_output.setdefault('WL_S1', wls[1])
                        device_output.setdefault('WL_S2', wls[2])
                    else:
                        device_output = device.get_tension_fav_ex(0, 0, 0, True)
                        device_output.setdefault('WL_T', None)
                        device_output.setdefault('WL_S1', None)
                        device_output.setdefault('WL_S2', None)

                    device_output.setdefault('Time', measurement_time)

                    # logging.info(u'Measurement is ready ' + str(device_output))

                    # ждем освобождения буфера
                    while raw_measurements_buffer['is_locked']:
                        await asyncio.sleep(0.1)

                    # блокируем буфер для записи в него
                    raw_measurements_buffer['is_locked'] = True
                    try:
                        raw_measurements_buffer.setdefault(measurement_time, []).append(device_output)
                        # logging.info(u'Measurement saved')

                    finally:
                        # разблокируем буфер
                        raw_measurements_buffer['is_locked'] = False

            # сырые измерения учтены, их можно удалять
            for cur_output_time in list(wls_buffer.keys()):
                if cur_output_time == 'is_locked':
                    continue
                del wls_buffer[cur_output_time]

        finally:
            wls_buffer['is_locked'] = False



async def save_measurements_to_db():
    global raw_measurements_buffer, instrument_descr

    while True:
        await asyncio.sleep(0.1)

        # ждем появления данных в буфере
        while len(raw_measurements_buffer.items()) < 2:
            await asyncio.sleep(1)

        # ждем освобождения буфера
        while raw_measurements_buffer['is_locked']:
            await asyncio.sleep(0.1)

        # блокируем буфер (чтобы надежно прочитать его)
        raw_measurements_buffer['is_locked'] = True
        try:
            # по всем записям в raw_measurements_buffer
            for (cur_output_time, devices_output) in raw_measurements_buffer.items():
                if cur_output_time == 'is_locked':
                    continue
                    
                # время усредненного блока, в которое попадает это измерение
                averaged_block_time = cur_output_time - cur_output_time % (1 / instrument_descr['SampleRate'])

                # создаем запись с таким временем или добавляем в существующую
                cur_mean_block = averaged_measurements.setdefault(averaged_block_time, len(devices_output)*15*[0.0])

                """
                Порядок вывода измерений
                    00  unix_timestamp
                    
                device0
                    0	num_of_measurements
                    1	t
                    2	t_std
                    3	f_av
                    4	f_av_std
                    5	f_bend
                    6	f_bend_std
                    7	ice
                    8	ice_std
                    
                device1
                    9	num_of_measurements
                    10	t
                    11	t_std
                    12	f_av
                    13	f_av_std
                    14	f_bend
                    15	f_bend_std
                    16	ice
                    17	ice_std

                device2
                    18	num_of_measurements
                    19	t
                    ...
                """
                measurements_output_order = {'T_degC': 1, 'Fav_N': 3, 'Fbend_N': 5, 'Ice_mm': 7, 'WL_T': 9, 'WL_S1': 11, 'WL_S2': 13}
                measurements_output_size = 2*len(measurements_output_order) + 1

                # по всем измерениям текущего измерителя
                for device_num, cur_output in enumerate(devices_output):

                    # пустые измерения пропускаем
                    if not cur_output['T_degC']:
                        continue

                    # усредняем поля из списка
                    print(len(cur_mean_block), 0 + device_num * measurements_output_size)
                    i = cur_mean_block[0 + device_num * measurements_output_size]
                    for name, index in measurements_output_order.items():
                        # итерационное среднее
                        cur_mean_block[index + device_num * measurements_output_size] = (cur_mean_block[index + device_num * measurements_output_size]*i + cur_output[name]) \
                                                                                        / (i+1)
                    cur_mean_block[0 + device_num * measurements_output_size] += 1

            # выводим усредненные измерения в файл
            key_to_be_deleted = list(averaged_measurements.keys())[:-1]
            for averaged_block_time in sorted(key_to_be_deleted):
                one_measurement = [averaged_block_time] + averaged_measurements[averaged_block_time]

                print('New averaged data from x55', averaged_block_time, averaged_measurements[averaged_block_time][3], averaged_measurements[averaged_block_time][0])

                data_saved = False

                # сохранение текущего блока измерений в файл (если файл не занят)
                try:
                    with open(out_file_name, 'a') as file:
                        file.write(str(one_measurement).strip('[]') + '\n')
                except IOError:
                    pass
                else:
                    # удаляем запись, она сохранена
                    del averaged_measurements[averaged_block_time]

            # сырые измерения учтены, их можно удалять из raw_measurements_buffer
            for cur_output_time in list(raw_measurements_buffer.keys()):
                if cur_output_time == 'is_locked':
                    continue
                del raw_measurements_buffer[cur_output_time]

        except Exception as e:
            return_error(e)
        finally:
            # разблокируем буфер
            raw_measurements_buffer['is_locked'] = False

            # спим
            await asyncio.sleep(int(1.0 / instrument_descr['SampleRate']))


def run_server(loop=None):
    loop = asyncio.get_event_loop()

    # функция получает длины волн от x55 c исходной частотой и складывает их во временный буффер
    asyncio.async(get_data_from_x55_coroutine())

    # конвертирует длины волн в измерения
    asyncio.async(convert_wl_to_device())

    # функция усредняет измерения и записывает их в БД
    asyncio.async(save_measurements_to_db())

    loop.run_forever()


if __name__ == "__main__":

    # очистка файла для усредненных измерений
    file_cleaned = False
    print('Cleaning outdata-file...')
    while not file_cleaned:
        try:
            with open(out_file_name, 'r+') as file:
                file.truncate(0)
        except IOError:
            pass
        else:
            file_cleaned = True
    print('done')

    logging.basicConfig(format=u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s', level=logging.DEBUG)  # , filename=u'UPK_server.log')
    logging.info(u'Start file')
    run_server()
