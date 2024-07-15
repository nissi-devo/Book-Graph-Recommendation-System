def is_valid_book(book):
    """
    Validates the structure and data types of a book metadata JSON object.
    """
    try:
        if not isinstance(book, dict):
            print(f"Invalid book structure: {book}")
            return False

        # Validate primitive fields
        if not isinstance(book.get('asin'), str):
            return False
        if not isinstance(book.get('price'), (int, float)):
            return False
        if not isinstance(book.get('imUrl'), str):
            return False

        # Validate 'related' field
        if 'related' in book:
            related = book['related']
            if not isinstance(related, dict):
                return False
            if 'also_bought' in related and (not isinstance(related['also_bought'], list) or not all(
                    isinstance(i, str) for i in related['also_bought'])):
                return False
            if 'buy_after_viewing' in related and (not isinstance(related['buy_after_viewing'], list) or not all(
                    isinstance(i, str) for i in related['buy_after_viewing'])):
                return False
            if 'also_viewed' in related and (not isinstance(related['also_viewed'], list) or not all(
                    isinstance(i, str) for i in related['also_viewed'])):
                return False

        # Validate 'categories' field
        if 'categories' in book:
            categories = book['categories']
            if not isinstance(categories, list) or not all(
                    isinstance(cat_list, list) and all(isinstance(cat, str) for cat in cat_list) for cat_list in
                    categories):
                return False

        return True

    except Exception as e:
        print(f"Exception during validation: {e}")
        return False

def is_valid_review(review):
    """
    Validates the structure and data types of a review JSON object.
    """
    if not isinstance(review, dict):
        return False

    # Validate primitive fields
    if not all(isinstance(review.get(key), str) for key in ['reviewerID', 'asin', 'reviewerName', 'reviewText', 'summary', 'reviewTime']):
        return False
    if not isinstance(review.get('overall'), (int, float)):
        return False
    if not isinstance(review.get('unixReviewTime'), int):
        return False
    if not isinstance(review.get('helpful'), list) or not all(isinstance(i, int) for i in review['helpful']):
        return False

    return True

