DO $$
DECLARE
    domains TEXT[] = ARRAY[
        'gmail.com', 'yahoo.com', 'outlook.com', 'protonmail.com', 'zoho.com',
        'mail.com', 'aol.com', 'icloud.com', 'fastmail.com', 'tutanota.com',
        'mail.ru', 'yandex.ru', 'rambler.ru', 'bk.ru', 'list.ru',
        'inbox.ru', 'ya.ru', 'gmail.by', 'mail.ua', 'meta.ua',
        'почта.рф', 'яндекс.рф', 'маил.рф', 'бел.бел', 'укр.укр',
        'католик.рф', 'домен.рус', 'сайт.бел', 'онлайн.укр', 'кремль.рф',
        'i-love-you.org', 'hacker.me', 'artists.world', 'musician.net',
        'writers.club', 'developers.team', 'engineer.space', 'scientist.tech'
    ];

    first_names TEXT[] = ARRAY[
        'leonardo', 'vincent', 'claude', 'rembrandt', 'johann',
        'wolfgang', 'franz', 'ludwig', 'frederic', 'antonio',
        'william', 'alexander', 'homer', 'john', 'dante',
        'alexei', 'ivan', 'sergey', 'nikolai', 'boris',
        'pyotr', 'dmitry', 'mikhail', 'andrei', 'vladimir',
        'ekaterina', 'olga', 'anna', 'maria', 'natalia',
        'oleksandr', 'volodymyr', 'vasyl', 'mykola', 'taras',
        'yaroslav', 'bohdan', 'sviatoslav', 'nazar', 'rostislav',
        'sophia', 'victoria', 'isabella', 'emma', 'olivia',
        'anastasia', 'irina', 'svetlana', 'lyudmila', 'tatyana'
    ];

    last_names TEXT[] = ARRAY[
        'da_vinci', 'vangogh', 'monet', 'harmenszoon', 'bach',
        'mozart', 'schubert', 'beethoven', 'chopin', 'vivaldi',
        'shakespeare', 'pushkin', 'simpson', 'milton', 'alighieri',
        'romanov', 'petrov', 'sidorov', 'lebedev', 'smirnov',
        'chekhov', 'dostoevsky', 'turgenev', 'gogol', 'bulgakov',
        'tolstoy', 'nabokov', 'solzhenitsyn', 'pasternak', 'akunin',
        'shevchenko', 'franko', 'lesya', 'skovoroda', 'kostenko',
        'bykov', 'korotkevich', 'bogdanovich', 'kupala', 'kolas',
        'coder', 'developer', 'engineer', 'scientist', 'artist',
        'musician', 'writer', 'philosopher', 'explorer', 'inventor'
    ];

    prefixes TEXT[] = ARRAY['', 'the_', 'real_', 'official_', 'my_', 'super_', 'best_'];
    suffixes TEXT[] = ARRAY['', '123', '2023', '007', '88', '42', '99', 'x', 'z', 'jr', 'sr'];
    separators TEXT[] = ARRAY['.', '-', '_', ''];

    rec RECORD;
    e_mail TEXT;
    local_part TEXT;
    domain TEXT;
    use_prefix BOOLEAN;
    use_suffix BOOLEAN;
    use_digits BOOLEAN;
    name_format INT;
BEGIN
    FOR rec IN SELECT id FROM backup_users LOOP
        domain = domains[1 + floor(random() * array_length(domains, 1))];

        use_prefix = random() > 0.7;
        use_suffix = random() > 0.6;
        use_digits = random() > 0.5;
        name_format = floor(random() * 4);

        local_part = first_names[1 + floor(random() * array_length(first_names, 1))];

        CASE name_format
            WHEN 0 THEN -- имя.фамилия
                local_part = local_part || separators[1 + floor(random() * array_length(separators, 1))] ||
                             last_names[1 + floor(random() * array_length(last_names, 1))];
            WHEN 1 THEN -- и.фамилия
                local_part = substring(local_part from 1 for 1) || separators[1 + floor(random() * array_length(separators, 1))] ||
                             last_names[1 + floor(random() * array_length(last_names, 1))];
            WHEN 2 THEN -- фамилия.и
                local_part = last_names[1 + floor(random() * array_length(last_names, 1))] ||
                             separators[1 + floor(random() * array_length(separators, 1))] ||
                             substring(local_part from 1 for 1);
            ELSE -- имя+цифры
                local_part = local_part || floor(random() * 1000)::text;
        END CASE;

        IF use_prefix THEN
            local_part = prefixes[1 + floor(random() * array_length(prefixes, 1))] || local_part;
        END IF;

        IF use_suffix THEN
            local_part = local_part || suffixes[1 + floor(random() * array_length(suffixes, 1))];
        END IF;

        IF use_digits AND name_format != 2 THEN
            IF random() > 0.5 THEN
                local_part = local_part || floor(random() * 100)::text;
            ELSE
                local_part = floor(random() * 100)::text || local_part;
            END IF;
        END IF;

        e_mail = lower(local_part) || '@' || domain;

        UPDATE backup_users SET email = e_mail WHERE id = rec.id;
    END LOOP;
END $$;