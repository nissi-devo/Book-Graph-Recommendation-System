def preprocess_categories(categories):
    processed_categories = {}
    for category_path in categories:
        for category in category_path:
            # Exclude non-informative categories
            if category not in ["Books", "Kindle eBooks", "Kindle Store"]:
                processed_categories[category] = processed_categories.get(category, 0) + 1

    # If there are no categories
    if not processed_categories:
        return {'main_category': None, 'sub-categories': []}

    # Determine the most common category
    main_category = max(processed_categories, key=processed_categories.get)
    # Gather all other categories as sub-categories, excluding the main category
    sub_categories = [category for category in processed_categories if category != main_category]

    return {'main_category': main_category, 'sub_categories': sub_categories}