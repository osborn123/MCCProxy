import pandas as pd


def extract_features(raw_features: pd.DataFrame) -> pd.DataFrame:
    extracted_features = pd.DataFrame()
    extracted_features['read_arrive_times_p30'] = raw_features.iloc[:, 0].apply(
        lambda x: get_num_accesses_last_k_time_steps(x, 30))
    extracted_features['read_arrive_times_p50'] = raw_features.iloc[:, 0].apply(
        lambda x: get_num_accesses_last_k_time_steps(x, 50))
    extracted_features['read_arrive_times_p100'] = raw_features.iloc[:, 0].apply(
        lambda x: get_num_accesses_last_k_time_steps(x, 100))
    extracted_features['read_arrive_times_p200'] = raw_features.iloc[:, 0].apply(
        lambda x: get_num_accesses_last_k_time_steps(x, 200))

    read_p_delta_5_array = raw_features.iloc[:, 0].apply(lambda x: get_num_steps_between_accesses(x, 6))

    # Assume df is your DataFrame and 'list_column' is the column with lists
    extracted_features[
        ['read_p_delta0', 'read_p_delta1', 'read_p_delta2', 'read_p_delta3', 'read_p_delta4', 'read_p_delta5']] \
        = read_p_delta_5_array.apply(pd.Series)

    extracted_features['read_p_delta0_delta1'] = extracted_features['read_p_delta0'] - extracted_features[
        'read_p_delta1']
    extracted_features['read_p_delta1_delta2'] = extracted_features['read_p_delta1'] - extracted_features[
        'read_p_delta2']
    extracted_features['read_p_delta2_delta3'] = extracted_features['read_p_delta2'] - extracted_features[
        'read_p_delta3']
    extracted_features['read_p_delta3_delta4'] = extracted_features['read_p_delta3'] - extracted_features[
        'read_p_delta4']
    extracted_features['read_p_delta4_delta5'] = extracted_features['read_p_delta4'] - extracted_features[
        'read_p_delta5']

    # write features
    extracted_features['write_arrive_times_p30'] = raw_features.iloc[:, 1].apply(
        lambda x: get_num_accesses_last_k_time_steps(x, 30))
    extracted_features['write_arrive_times_p50'] = raw_features.iloc[:, 1].apply(
        lambda x: get_num_accesses_last_k_time_steps(x, 50))
    extracted_features['write_arrive_times_p100'] = raw_features.iloc[:, 1].apply(
        lambda x: get_num_accesses_last_k_time_steps(x, 100))
    extracted_features['write_arrive_times_p200'] = raw_features.iloc[:, 1].apply(
        lambda x: get_num_accesses_last_k_time_steps(x, 200))

    write_p_delta_5_array = raw_features.iloc[:, 1].apply(lambda x: get_num_steps_between_accesses(x, 6))

    extracted_features[
        ['write_p_delta0', 'write_p_delta1', 'write_p_delta2', 'write_p_delta3', 'write_p_delta4', 'write_p_delta5']] \
        = write_p_delta_5_array.apply(pd.Series)

    extracted_features['write_p_delta0_delta1'] = extracted_features['write_p_delta0'] - extracted_features[
        'write_p_delta1']
    extracted_features['write_p_delta1_delta2'] = extracted_features['write_p_delta1'] - extracted_features[
        'write_p_delta2']
    extracted_features['write_p_delta2_delta3'] = extracted_features['write_p_delta2'] - extracted_features[
        'write_p_delta3']
    extracted_features['write_p_delta3_delta4'] = extracted_features['write_p_delta3'] - extracted_features[
        'write_p_delta4']
    extracted_features['write_p_delta4_delta5'] = extracted_features['write_p_delta4'] - extracted_features[
        'write_p_delta5']

    return extracted_features


def get_num_accesses_last_k_time_steps(access_array, k):
    count = 0
    for i in range(min(len(access_array), k // 64)):
        count += access_array[i].bit_count()
    if k % 64 != 0 and len(access_array) > k // 64:
        count += (access_array[k // 64] & ((1 << (k % 64)) - 1)).bit_count()
    return count


def get_num_steps_between_accesses(access_array, k):
    distances = [0] * k
    last_one_pos = -1
    i = j = 0
    while i < k and j < len(access_array):
        mask = access_array[j]
        while mask != 0 and i < k:
            pos = trailing_zeros(mask) + j * 64
            distances[i] = pos - last_one_pos
            i += 1
            last_one_pos = pos
            mask &= mask - 1
        j += 1
    return distances


def trailing_zeros(x):
    return (x & -x).bit_length() - 1
